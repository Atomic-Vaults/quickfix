//Package userrequest msg type = BE.
package userrequest

import (
	"github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/errors"
	"github.com/quickfixgo/quickfix/fix"
	"github.com/quickfixgo/quickfix/fix/field"
)

import (
	"github.com/quickfixgo/quickfix/fix/enum"
)

//Message is a UserRequest wrapper for the generic Message type
type Message struct {
	quickfix.Message
}

//UserRequestID is a required field for UserRequest.
func (m Message) UserRequestID() (*field.UserRequestIDField, errors.MessageRejectError) {
	f := &field.UserRequestIDField{}
	err := m.Body.Get(f)
	return f, err
}

//GetUserRequestID reads a UserRequestID from UserRequest.
func (m Message) GetUserRequestID(f *field.UserRequestIDField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//UserRequestType is a required field for UserRequest.
func (m Message) UserRequestType() (*field.UserRequestTypeField, errors.MessageRejectError) {
	f := &field.UserRequestTypeField{}
	err := m.Body.Get(f)
	return f, err
}

//GetUserRequestType reads a UserRequestType from UserRequest.
func (m Message) GetUserRequestType(f *field.UserRequestTypeField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//Username is a required field for UserRequest.
func (m Message) Username() (*field.UsernameField, errors.MessageRejectError) {
	f := &field.UsernameField{}
	err := m.Body.Get(f)
	return f, err
}

//GetUsername reads a Username from UserRequest.
func (m Message) GetUsername(f *field.UsernameField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//Password is a non-required field for UserRequest.
func (m Message) Password() (*field.PasswordField, errors.MessageRejectError) {
	f := &field.PasswordField{}
	err := m.Body.Get(f)
	return f, err
}

//GetPassword reads a Password from UserRequest.
func (m Message) GetPassword(f *field.PasswordField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//NewPassword is a non-required field for UserRequest.
func (m Message) NewPassword() (*field.NewPasswordField, errors.MessageRejectError) {
	f := &field.NewPasswordField{}
	err := m.Body.Get(f)
	return f, err
}

//GetNewPassword reads a NewPassword from UserRequest.
func (m Message) GetNewPassword(f *field.NewPasswordField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//RawDataLength is a non-required field for UserRequest.
func (m Message) RawDataLength() (*field.RawDataLengthField, errors.MessageRejectError) {
	f := &field.RawDataLengthField{}
	err := m.Body.Get(f)
	return f, err
}

//GetRawDataLength reads a RawDataLength from UserRequest.
func (m Message) GetRawDataLength(f *field.RawDataLengthField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//RawData is a non-required field for UserRequest.
func (m Message) RawData() (*field.RawDataField, errors.MessageRejectError) {
	f := &field.RawDataField{}
	err := m.Body.Get(f)
	return f, err
}

//GetRawData reads a RawData from UserRequest.
func (m Message) GetRawData(f *field.RawDataField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//EncryptedPasswordMethod is a non-required field for UserRequest.
func (m Message) EncryptedPasswordMethod() (*field.EncryptedPasswordMethodField, errors.MessageRejectError) {
	f := &field.EncryptedPasswordMethodField{}
	err := m.Body.Get(f)
	return f, err
}

//GetEncryptedPasswordMethod reads a EncryptedPasswordMethod from UserRequest.
func (m Message) GetEncryptedPasswordMethod(f *field.EncryptedPasswordMethodField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//EncryptedPasswordLen is a non-required field for UserRequest.
func (m Message) EncryptedPasswordLen() (*field.EncryptedPasswordLenField, errors.MessageRejectError) {
	f := &field.EncryptedPasswordLenField{}
	err := m.Body.Get(f)
	return f, err
}

//GetEncryptedPasswordLen reads a EncryptedPasswordLen from UserRequest.
func (m Message) GetEncryptedPasswordLen(f *field.EncryptedPasswordLenField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//EncryptedPassword is a non-required field for UserRequest.
func (m Message) EncryptedPassword() (*field.EncryptedPasswordField, errors.MessageRejectError) {
	f := &field.EncryptedPasswordField{}
	err := m.Body.Get(f)
	return f, err
}

//GetEncryptedPassword reads a EncryptedPassword from UserRequest.
func (m Message) GetEncryptedPassword(f *field.EncryptedPasswordField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//EncryptedNewPasswordLen is a non-required field for UserRequest.
func (m Message) EncryptedNewPasswordLen() (*field.EncryptedNewPasswordLenField, errors.MessageRejectError) {
	f := &field.EncryptedNewPasswordLenField{}
	err := m.Body.Get(f)
	return f, err
}

//GetEncryptedNewPasswordLen reads a EncryptedNewPasswordLen from UserRequest.
func (m Message) GetEncryptedNewPasswordLen(f *field.EncryptedNewPasswordLenField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//EncryptedNewPassword is a non-required field for UserRequest.
func (m Message) EncryptedNewPassword() (*field.EncryptedNewPasswordField, errors.MessageRejectError) {
	f := &field.EncryptedNewPasswordField{}
	err := m.Body.Get(f)
	return f, err
}

//GetEncryptedNewPassword reads a EncryptedNewPassword from UserRequest.
func (m Message) GetEncryptedNewPassword(f *field.EncryptedNewPasswordField) errors.MessageRejectError {
	return m.Body.Get(f)
}

//MessageBuilder builds UserRequest messages.
type MessageBuilder struct {
	quickfix.MessageBuilder
}

//Builder returns an initialized MessageBuilder with specified required fields for UserRequest.
func Builder(
	userrequestid *field.UserRequestIDField,
	userrequesttype *field.UserRequestTypeField,
	username *field.UsernameField) MessageBuilder {
	var builder MessageBuilder
	builder.MessageBuilder = quickfix.NewMessageBuilder()
	builder.Header().Set(field.NewBeginString(fix.BeginString_FIXT11))
	builder.Header().Set(field.NewDefaultApplVerID(enum.ApplVerID_FIX50SP2))
	builder.Header().Set(field.NewMsgType("BE"))
	builder.Body().Set(userrequestid)
	builder.Body().Set(userrequesttype)
	builder.Body().Set(username)
	return builder
}

//A RouteOut is the callback type that should be implemented for routing Message
type RouteOut func(msg Message, sessionID quickfix.SessionID) errors.MessageRejectError

//Route returns the beginstring, message type, and MessageRoute for this Mesage type
func Route(router RouteOut) (string, string, quickfix.MessageRoute) {
	r := func(msg quickfix.Message, sessionID quickfix.SessionID) errors.MessageRejectError {
		return router(Message{msg}, sessionID)
	}
	return enum.ApplVerID_FIX50SP2, "BE", r
}