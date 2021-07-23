// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package shim

import (
	"sync"
)

// Ensure, that partitionDelegateMock does implement partitionDelegate.
// If this is not the case, regenerate this file with moq.
var _ partitionDelegate = &partitionDelegateMock{}

// partitionDelegateMock is a mock implementation of partitionDelegate.
//
// 	func TestSomethingThatUsespartitionDelegate(t *testing.T) {
//
// 		// make and configure a mocked partitionDelegate
// 		mockedpartitionDelegate := &partitionDelegateMock{
// 			broadcastFunc: func(msg partitionMsg)  {
// 				panic("mock out the broadcast method")
// 			},
// 			startFunc: func()  {
// 				panic("mock out the startJoin method")
// 			},
// 			stopFunc: func()  {
// 				panic("mock out the stop method")
// 			},
// 		}
//
// 		// use mockedpartitionDelegate in code that requires partitionDelegate
// 		// and then make assertions.
//
// 	}
type partitionDelegateMock struct {
	// broadcastFunc mocks the broadcast method.
	broadcastFunc func(msg partitionMsg)

	// startFunc mocks the startJoin method.
	startFunc func()

	// stopFunc mocks the stop method.
	stopFunc func()

	// calls tracks calls to the methods.
	calls struct {
		// broadcast holds details about calls to the broadcast method.
		broadcast []struct {
			// Msg is the msg argument value.
			Msg partitionMsg
		}
		// startJoin holds details about calls to the startJoin method.
		start []struct {
		}
		// stop holds details about calls to the stop method.
		stop []struct {
		}
	}
	lockbroadcast sync.RWMutex
	lockstart     sync.RWMutex
	lockstop      sync.RWMutex
}

// broadcast calls broadcastFunc.
func (mock *partitionDelegateMock) broadcast(msg partitionMsg) {
	if mock.broadcastFunc == nil {
		panic("partitionDelegateMock.broadcastFunc: method is nil but partitionDelegate.broadcast was just called")
	}
	callInfo := struct {
		Msg partitionMsg
	}{
		Msg: msg,
	}
	mock.lockbroadcast.Lock()
	mock.calls.broadcast = append(mock.calls.broadcast, callInfo)
	mock.lockbroadcast.Unlock()
	mock.broadcastFunc(msg)
}

// broadcastCalls gets all the calls that were made to broadcast.
// Check the length with:
//     len(mockedpartitionDelegate.broadcastCalls())
func (mock *partitionDelegateMock) broadcastCalls() []struct {
	Msg partitionMsg
} {
	var calls []struct {
		Msg partitionMsg
	}
	mock.lockbroadcast.RLock()
	calls = mock.calls.broadcast
	mock.lockbroadcast.RUnlock()
	return calls
}

// startJoin calls startFunc.
func (mock *partitionDelegateMock) start() {
	if mock.startFunc == nil {
		panic("partitionDelegateMock.startFunc: method is nil but partitionDelegate.startJoin was just called")
	}
	callInfo := struct {
	}{}
	mock.lockstart.Lock()
	mock.calls.start = append(mock.calls.start, callInfo)
	mock.lockstart.Unlock()
	mock.startFunc()
}

// startCalls gets all the calls that were made to startJoin.
// Check the length with:
//     len(mockedpartitionDelegate.startCalls())
func (mock *partitionDelegateMock) startCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockstart.RLock()
	calls = mock.calls.start
	mock.lockstart.RUnlock()
	return calls
}

// stop calls stopFunc.
func (mock *partitionDelegateMock) stop() {
	if mock.stopFunc == nil {
		panic("partitionDelegateMock.stopFunc: method is nil but partitionDelegate.stop was just called")
	}
	callInfo := struct {
	}{}
	mock.lockstop.Lock()
	mock.calls.stop = append(mock.calls.stop, callInfo)
	mock.lockstop.Unlock()
	mock.stopFunc()
}

// stopCalls gets all the calls that were made to stop.
// Check the length with:
//     len(mockedpartitionDelegate.stopCalls())
func (mock *partitionDelegateMock) stopCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockstop.RLock()
	calls = mock.calls.stop
	mock.lockstop.RUnlock()
	return calls
}
