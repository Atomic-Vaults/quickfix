package quickfix

import (
	"testing"
	"time"

	"github.com/quickfixgo/quickfix/enum"
	"github.com/quickfixgo/quickfix/internal"
	"github.com/stretchr/testify/suite"
)

type LogonStateTestSuite struct {
	SessionSuiteRig
}

func TestLogonStateTestSuite(t *testing.T) {
	suite.Run(t, new(LogonStateTestSuite))
}

func (s *LogonStateTestSuite) SetupTest() {
	s.Init()
	s.session.stateMachine.State = logonState{}
}

func (s *LogonStateTestSuite) TestPreliminary() {
	s.False(s.session.IsLoggedOn())
	s.True(s.session.IsConnected())
	s.True(s.session.IsSessionTime())
}

func (s *LogonStateTestSuite) TestTimeoutLogonTimeout() {
	s.Timeout(s.session, internal.LogonTimeout)
	s.State(latentState{})
}

func (s *LogonStateTestSuite) TestTimeoutLogonTimeoutInitiatedLogon() {
	s.session.initiateLogon = true

	s.MockApp.On("OnLogout")
	s.Timeout(s.session, internal.LogonTimeout)

	s.MockApp.AssertExpectations(s.T())
	s.State(latentState{})
}

func (s *LogonStateTestSuite) TestTimeoutNotLogonTimeout() {
	tests := []internal.Event{internal.PeerTimeout, internal.NeedHeartbeat, internal.LogoutTimeout}

	for _, test := range tests {
		s.Timeout(s.session, test)
		s.State(logonState{})
	}
}

func (s *LogonStateTestSuite) TestDisconnected() {
	s.session.Disconnected(s.session)
	s.State(latentState{})
}

func (s *LogonStateTestSuite) TestFixMsgInNotLogon() {
	s.fixMsgIn(s.session, s.NewOrderSingle())

	s.MockApp.AssertExpectations(s.T())
	s.State(latentState{})
	s.NextTargetMsgSeqNum(1)
}

func (s *LogonStateTestSuite) TestFixMsgInLogon() {
	s.store.IncrNextSenderMsgSeqNum()
	s.MessageFactory.seqNum = 1
	s.store.IncrNextTargetMsgSeqNum()

	logon := s.Logon()
	logon.Body.SetField(tagHeartBtInt, FIXInt(32))

	s.MockApp.On("FromAdmin").Return(nil)
	s.MockApp.On("OnLogon")
	s.MockApp.On("ToAdmin")
	s.fixMsgIn(s.session, logon)

	s.MockApp.AssertExpectations(s.T())

	s.State(inSession{})
	s.Equal(32*time.Second, s.session.heartBtInt)

	s.LastToAdminMessageSent()
	s.MessageType(enum.MsgType_LOGON, s.MockApp.lastToAdmin)
	s.FieldEquals(tagHeartBtInt, 32, s.MockApp.lastToAdmin.Body)

	s.NextTargetMsgSeqNum(3)
	s.NextSenderMsgSeqNum(3)
}

func (s *LogonStateTestSuite) TestFixMsgInLogonInitiateLogon() {
	s.session.initiateLogon = true
	s.store.IncrNextSenderMsgSeqNum()
	s.MessageFactory.seqNum = 1
	s.store.IncrNextTargetMsgSeqNum()

	logon := s.Logon()
	logon.Body.SetField(tagHeartBtInt, FIXInt(32))

	s.MockApp.On("FromAdmin").Return(nil)
	s.MockApp.On("OnLogon")
	s.fixMsgIn(s.session, logon)

	s.MockApp.AssertExpectations(s.T())
	s.State(inSession{})

	s.NextTargetMsgSeqNum(3)
	s.NextSenderMsgSeqNum(2)
}

func (s *LogonStateTestSuite) TestStop() {
	var tests = []bool{true, false}

	for _, doInitiateLogon := range tests {
		s.SetupTest()
		s.session.initiateLogon = doInitiateLogon

		if doInitiateLogon {
			s.MockApp.On("OnLogout")
		}

		s.session.Stop(s.session)
		s.MockApp.AssertExpectations(s.T())
		s.Disconnected()
		s.Stopped()
	}
}