// Copyright (c) 2020 Andy Pan
// Copyright (c) 2017 Max Riveiro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux || freebsd || dragonfly || darwin
// +build linux freebsd dragonfly darwin

package socket

import (
	"net"
	"os"
	"syscall"

	"github.com/mushanyux/MSIM/pkg/errors"
)

var listenerBacklogMaxSize = maxListenerBacklog()

// GetTCPSockAddr the structured addresses based on the protocol and raw address.
func GetTCPSockAddr(proto, addr string) (sa syscall.Sockaddr, family int, tcpAddr *net.TCPAddr, ipv6only bool, err error) {
	var tcpVersion string

	tcpAddr, err = net.ResolveTCPAddr(proto, addr)
	if err != nil {
		return
	}

	tcpVersion, err = determineTCPProto(proto, tcpAddr)
	if err != nil {
		return
	}

	switch tcpVersion {
	case "tcp4":
		family = syscall.AF_INET
		sa, err = ipToSockaddr(family, tcpAddr.IP, tcpAddr.Port, "")
	case "tcp6":
		ipv6only = true
		fallthrough
	case "tcp":
		family = syscall.AF_INET6
		sa, err = ipToSockaddr(family, tcpAddr.IP, tcpAddr.Port, tcpAddr.Zone)
	default:
		err = errors.ErrUnsupportedProtocol
	}

	return
}

func determineTCPProto(proto string, addr *net.TCPAddr) (string, error) {
	// If the protocol is set to "tcp", we try to determine the actual protocol
	// version from the size of the resolved IP address. Otherwise, we simple use
	// the protocol given to us by the caller.

	if addr.IP.To4() != nil {
		return "tcp4", nil
	}

	if addr.IP.To16() != nil {
		return "tcp6", nil
	}

	switch proto {
	case "tcp", "tcp4", "tcp6":
		return proto, nil
	}

	return "", errors.ErrUnsupportedTCPProtocol
}

// tcpSocket creates an endpoint for communication and returns a file descriptor that refers to that endpoint.
// Argument `reusePort` indicates whether the SO_REUSEPORT flag will be assigned.
func tcpSocket(proto, addr string, passive bool, sockOpts ...Option) (fd int, netAddr net.Addr, err error) {
	var (
		family   int
		ipv6only bool
		sa       syscall.Sockaddr
	)

	if sa, family, netAddr, ipv6only, err = GetTCPSockAddr(proto, addr); err != nil {
		return
	}

	if fd, err = sysSocket(family, syscall.SOCK_STREAM, syscall.IPPROTO_TCP); err != nil {
		err = os.NewSyscallError("socket", err)
		return
	}
	defer func() {
		// ignore EINPROGRESS for non-blocking socket connect, should be processed by caller
		if err != nil {
			if err, ok := err.(*os.SyscallError); ok && err.Err == syscall.EINPROGRESS {
				return
			}
			_ = syscall.Close(fd)
		}
	}()

	if family == syscall.AF_INET6 && ipv6only {
		if err = SetIPv6Only(fd, 1); err != nil {
			return
		}
	}

	for _, sockOpt := range sockOpts {
		if err = sockOpt.SetSockOpt(fd, sockOpt.Opt); err != nil {
			return
		}
	}

	if passive {
		if err = os.NewSyscallError("bind", syscall.Bind(fd, sa)); err != nil {
			return
		}
		// Set backlog size to the maximum.
		err = os.NewSyscallError("listen", syscall.Listen(fd, listenerBacklogMaxSize))
	} else {
		err = os.NewSyscallError("connect", syscall.Connect(fd, sa))
	}

	return
}
