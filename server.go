// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fluentforwardreceiver

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/tinylib/msgp/msgp"
	"go.opencensus.io/stats"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/internal/data"

	otlptrace "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/trace/v1"
	"go.opentelemetry.io/collector/receiver/fluentforwardreceiver/observ"
	"go.uber.org/zap"
)

// The initial size of the read buffer. Messages can come in that are bigger
// than this, but this serves as a starting point.
const readBufferSize = 10 * 1024

type server struct {
	outCh   chan<- Event
	traceCh chan<- pdata.Traces
	logger  *zap.Logger
}

func newServer(outCh chan<- Event, traceCh chan<- pdata.Traces, logger *zap.Logger) *server {
	return &server{
		outCh:   outCh,
		traceCh: traceCh,
		logger:  logger,
	}
}

func (s *server) Start(ctx context.Context, listener net.Listener) {
	go func() {
		s.handleConnections(ctx, listener)
		if ctx.Err() == nil {
			panic("logic error in receiver, connections should always be listened for while receiver is running")
		}
	}()
}

func (s *server) handleConnections(ctx context.Context, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if ctx.Err() != nil {
			return
		}
		// If there is an error and the receiver isn't shutdown, we need to
		// keep trying to accept connections if at all possible. Put in a sleep
		// to prevent hot loops in case the error persists.
		if err != nil {
			time.Sleep(10 * time.Second)
			continue
		}
		stats.Record(ctx, observ.ConnectionsOpened.M(1))

		s.logger.Debug("Got connection", zap.String("remoteAddr", conn.RemoteAddr().String()))

		go func() {
			defer stats.Record(ctx, observ.ConnectionsClosed.M(1))

			err := s.handleConn(ctx, conn)
			if err != nil {
				if err == io.EOF {
					s.logger.Debug("Closing connection", zap.String("remoteAddr", conn.RemoteAddr().String()), zap.Error(err))
				} else {
					s.logger.Debug("Unexpected error handling connection", zap.String("remoteAddr", conn.RemoteAddr().String()), zap.Error(err))
				}
			}
			conn.Close()
		}()
	}
}

func (s *server) handleConn(ctx context.Context, conn net.Conn) error {
	reader := msgp.NewReaderSize(conn, readBufferSize)

	for {
		mode, err := DetermineNextEventMode(reader.R)
		if err != nil {
			return err
		}

		var event Event
		switch mode {
		case UnknownMode:
			return errors.New("could not determine event mode")
		case MessageMode:
			event = &MessageEventLogRecord{}
		case ForwardMode:
			event = &ForwardEventLogRecords{}
		case PackedForwardMode:
			event = &PackedForwardEventLogRecords{}
		default:
			panic("programmer bug in mode handling")
		}
		tag, arrLen, err := readTag(reader)
		if err != nil {
			return fmt.Errorf("Error in reading tag: %v", err)
		}

		if tag == "data.span" {
			traces, err := parseSpans(reader)
			if err != nil {
				return fmt.Errorf("Error in parsing span: %v", err)
			}
			s.traceCh <- traces

		} else {
			err = event.DecodeMsg(reader, arrLen, tag)
			if err != nil {
				if err != io.EOF {
					stats.Record(ctx, observ.FailedToParse.M(1))
				}
				return fmt.Errorf("failed to parse %s mode event: %v", mode.String(), err)
			}

			stats.Record(ctx, observ.EventsParsed.M(1))

			s.outCh <- event

			// We must acknowledge the 'chunk' option if given. We could do this in
			// another goroutine if it is too much of a bottleneck to reading
			// messages -- this is the only thing that sends data back to the
			// client.
			if event.Chunk() != "" {
				err := msgp.Encode(conn, AckResponse{Ack: event.Chunk()})
				if err != nil {
					return fmt.Errorf("failed to acknowledge chunk %s: %v", event.Chunk(), err)
				}
			}
		}
	}
}

func readTag(dc *msgp.Reader) (string, uint32, error) {
	arrLen, err := dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return "", 0, err
	}

	tag, err := dc.ReadString()
	if err != nil {
		return "", 0, msgp.WrapError(err, "Tag")
	}
	return tag, arrLen, nil
}

func parseSpans(dc *msgp.Reader) (pdata.Traces, error) {
	var err error
	traceData := pdata.NewTraces()

	// number of spans to be exported
	spansLen, err := dc.ReadArrayHeader()
	if err != nil {
		return traceData, msgp.WrapError(err)
	}

	for i := 0; i < int(spansLen); i++ {
		_, err = dc.ReadArrayHeader()
		if err != nil {
			return traceData, msgp.WrapError(err, "Record")
		}

		// Since the first field is the end timestamp of the span and it is already present in the span field,
		// there is no need to read it

		err = dc.Skip()

		rss := traceData.ResourceSpans()
		rss.Resize(int(spansLen))

		err = fluentSpanToInternal(dc, rss, i)
		if err != nil {
			return traceData, err
		}
	}

	return traceData, nil
}

func fluentSpanToInternal(dc *msgp.Reader, rss pdata.ResourceSpansSlice, i int) error {
	rs := rss.At(i)

	ilss := rs.InstrumentationLibrarySpans()
	ilss.Resize(1)

	spanSlice := ilss.At(0).Spans()
	spanSlice.Resize(1)
	span := spanSlice.At(0)

	// Decode map of attributes
	attrLen, err := dc.ReadMapHeader()
	if err != nil {
		return msgp.WrapError(err, "Map")
	}
	for attrLen > 0 {
		attrLen--
		key, err := dc.ReadString()
		if err != nil {
			return msgp.WrapError(err, "Attribute key")
		}
		val, err := dc.ReadIntf()
		if err != nil {
			return msgp.WrapError(err, "Attribute value")
		}
		switch key {
		case "traceId":
			var traceID [16]byte
			decoded, err := hex.DecodeString(val.(string))
			if err != nil {
				return fmt.Errorf("Error while parsing TraceID: %v", err)
			}
			copy(traceID[:], decoded)
			span.SetTraceID(pdata.TraceID(data.NewTraceID(traceID)))
		case "spanId":
			var spanID [8]byte
			decoded, err := hex.DecodeString(val.(string))
			if err != nil {
				return fmt.Errorf("Error while parsing SpanID: %v", err)
			}
			copy(spanID[:], decoded)
			span.SetSpanID(pdata.NewSpanID(spanID))
		case "parentSpanId":
			var parentSpanID [8]byte
			if val.(string) == "0000000000000000" {
				parentSpanID = [8]byte{0}
			} else {
				decoded, err := hex.DecodeString(val.(string))
				if err != nil {
					return fmt.Errorf("Error while parsing ParentSpanID: %v", err)
				}
				copy(parentSpanID[:], decoded)
			}
			span.SetParentSpanID(pdata.NewSpanID(parentSpanID))
		case "name":
			span.SetName(val.(string))
		case "spanKind":
			span.SetKind(pdata.SpanKind(otlptrace.Span_SpanKind(val.(int64))))
		case "startTime":
			span.SetStartTime(pdata.TimestampUnixNano(val.(uint64)))
		case "endTime":
			span.SetEndTime(pdata.TimestampUnixNano(val.(uint64)))
		case "attrs":
			setAttributes(val.(map[string](interface{})), span.Attributes())
		case "droppedAttributesCount":
			span.SetDroppedAttributesCount(uint32(val.(int64)))
		case "links":
			err = linksToInternal(val.([]interface{}), span.Links())
			if err != nil {
				return fmt.Errorf("Error in setting links: %v", err)
			}
		case "droppedLinkCount":
			span.SetDroppedLinksCount(uint32(val.(int64)))
		case "messageEvents":
			eventsToInternal(val.([]interface{}), span)
		case "droppedMessageEventCount":
			span.SetDroppedEventsCount(uint32(val.(int64)))
		case "statusCode":
			code, _ := strconv.Atoi(val.(string))
			span.Status().SetCode(pdata.StatusCode(otlptrace.Status_StatusCode(code)))
		case "statusMessage":
			span.Status().SetMessage(val.(string))
		case "instrumentationLibraryName":
			ilss.At(0).InstrumentationLibrary().SetName(val.(string))
		case "instrumentationLibraryVersion":
			ilss.At(0).InstrumentationLibrary().SetVersion(val.(string))
		case "resource":
			setAttributes(val.(map[string](interface{})), rs.Resource().Attributes())
		default:
			return errors.New("Encountered unknown field while parsing span")
		}
	}
	return nil
}

func linksToInternal(fluentSpanLinks []interface{}, dest pdata.SpanLinkSlice) error {
	dest.Resize(len(fluentSpanLinks))
	for idx, val := range fluentSpanLinks {
		link := dest.At(idx)
		for k, v := range val.(map[string]interface{}) {
			switch k {
			case "attrs":
				setAttributes(v.(map[string]interface{}), link.Attributes())
			case "spanId":
				var spanID [8]byte
				copy(spanID[:], val.(string))
				link.SetSpanID(pdata.SpanID(data.NewSpanID(spanID)))
			case "traceId":
				var traceID [16]byte
				decoded, err := hex.DecodeString(v.(string))
				if err != nil {
					return fmt.Errorf("Error while parsing TraceID: %v", err)
				}
				copy(traceID[:], decoded)
				link.SetTraceID(pdata.TraceID(data.NewTraceID(traceID)))
			}
		}
	}
	return nil
}

func eventsToInternal(fluentMessageEvents []interface{}, dest pdata.Span) {
	events := dest.Events()
	events.Resize(len(fluentMessageEvents))
	for idx, val := range fluentMessageEvents {
		event := events.At(idx)
		for k, v := range val.(map[string]interface{}) {
			switch k {
			case "attrs":
				setAttributes(v.(map[string]interface{}), event.Attributes())
			case "ts":
				event.SetTimestamp(pdata.TimestampUnixNano(v.(uint64)))
			case "name":
				event.SetName(v.(string))
			}
		}
	}
}

func setAttributes(fluentAttrs map[string]interface{}, dest pdata.AttributeMap) {
	dest.InitEmptyWithCapacity(len(fluentAttrs))
	for k, v := range fluentAttrs {
		switch v.(type) {
		case string:
			dest.InsertString(k, v.(string))
		case uint64:
			dest.InsertInt(k, int64(v.(uint64)))
		}
	}
}

// DetermineNextEventMode inspects the next bit of data from the given peeker
// reader to determine which type of event mode it is.  According to the
// forward protocol spec: "Server MUST detect the carrier mode by inspecting
// the second element of the array."  It is assumed that peeker is aligned at
// the start of a new event, otherwise the result is undefined and will
// probably error.
func DetermineNextEventMode(peeker Peeker) (EventMode, error) {
	var chunk []byte
	var err error
	chunk, err = peeker.Peek(2)
	if err != nil {
		return UnknownMode, err
	}

	// The first byte is the array header, which will always be 1 byte since no
	// message modes have more than 4 entries. So skip to the second byte which
	// is the tag string header.
	tagType := chunk[1]
	// We already read the first type for the type
	tagLen := 1

	isFixStr := tagType&0b10100000 == 0b10100000
	if isFixStr {
		tagLen += int(tagType & 0b00011111)
	} else {
		switch tagType {
		case 0xd9:
			chunk, err = peeker.Peek(3)
			if err != nil {
				return UnknownMode, err
			}
			tagLen += 1 + int(chunk[2])
		case 0xda:
			chunk, err = peeker.Peek(4)
			if err != nil {
				return UnknownMode, err
			}
			tagLen += 2 + int(binary.BigEndian.Uint16(chunk[2:]))
		case 0xdb:
			chunk, err = peeker.Peek(6)
			if err != nil {
				return UnknownMode, err
			}
			tagLen += 4 + int(binary.BigEndian.Uint32(chunk[2:]))
		default:
			return UnknownMode, errors.New("malformed tag field")
		}
	}

	// Skip past the first byte (array header) and the entire tag and then get
	// one byte into the second field -- that is enough to know its type.
	chunk, err = peeker.Peek(1 + tagLen + 1)
	if err != nil {
		return UnknownMode, err
	}

	secondElmType := msgp.NextType(chunk[1+tagLen:])

	switch secondElmType {
	case msgp.IntType, msgp.UintType, msgp.ExtensionType:
		return MessageMode, nil
	case msgp.ArrayType:
		return ForwardMode, nil
	case msgp.BinType, msgp.StrType:
		return PackedForwardMode, nil
	default:
		return UnknownMode, fmt.Errorf("unable to determine next event mode for type %v", secondElmType)
	}
}
