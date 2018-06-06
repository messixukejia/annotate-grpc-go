/*
 *
 * Copyright 2014, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package grpc

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/transport"
)

var (
	// ErrClientConnClosing indicates that the operation is illegal because
	// the ClientConn is closing.
	ErrClientConnClosing = errors.New("grpc: the client connection is closing")
	// ErrClientConnTimeout indicates that the ClientConn cannot establish the
	// underlying connections within the specified timeout.
	// DEPRECATED: Please use context.DeadlineExceeded instead. This error will be
	// removed in Q1 2017.
	ErrClientConnTimeout = errors.New("grpc: timed out when dialing")

	// errNoTransportSecurity indicates that there is no transport security
	// being set for ClientConn. Users should either set one or explicitly
	// call WithInsecure DialOption to disable security.
	errNoTransportSecurity = errors.New("grpc: no transport security set (use grpc.WithInsecure() explicitly or set credentials)")
	// errTransportCredentialsMissing indicates that users want to transmit security
	// information (e.g., oauth2 token) which requires secure connection on an insecure
	// connection.
	errTransportCredentialsMissing = errors.New("grpc: the credentials require transport level security (use grpc.WithTransportCredentials() to set)")
	// errCredentialsConflict indicates that grpc.WithTransportCredentials()
	// and grpc.WithInsecure() are both called for a connection.
	errCredentialsConflict = errors.New("grpc: transport credentials are set for an insecure connection (grpc.WithTransportCredentials() and grpc.WithInsecure() are both called)")
	// errNetworkIO indicates that the connection is down due to some network I/O error.
	errNetworkIO = errors.New("grpc: failed with network I/O error")
	// errConnDrain indicates that the connection starts to be drained and does not accept any new RPCs.
	errConnDrain = errors.New("grpc: the connection is drained")
	// errConnClosing indicates that the connection is closing.
	// 这个表示连接被关闭了，一般是balancer中去掉之后，被teardown了
	errConnClosing = errors.New("grpc: the connection is closing")
	// errConnUnavailable indicates that the connection is unavailable.
	//这个表示连接是无效的，一般是服务有问题，但是没有从balancer中去掉，这个时候会一直重试连接
	errConnUnavailable = errors.New("grpc: the connection is unavailable")
	errNoAddr          = errors.New("grpc: there is no address available to dial")
	// minimum time to give a connection to complete
	minConnectTimeout = 20 * time.Second
)

// dialOptions configure a Dial call. dialOptions are set by the DialOption
// values passed to Dial.
type dialOptions struct {
	unaryInt  UnaryClientInterceptor
	streamInt StreamClientInterceptor
	codec     Codec
	cp        Compressor
	dc        Decompressor
	bs        backoffStrategy
	balancer  Balancer //负载均衡，自带的RoundRobin就会返回一个轮训策略的对象
	block     bool
	insecure  bool
	timeout   time.Duration
	scChan    <-chan ServiceConfig
	copts     transport.ConnectOptions
}

// DialOption configures how we set up the connection.
type DialOption func(*dialOptions)

// WithCodec returns a DialOption which sets a codec for message marshaling and unmarshaling.
func WithCodec(c Codec) DialOption {
	return func(o *dialOptions) {
		o.codec = c
	}
}

// WithCompressor returns a DialOption which sets a CompressorGenerator for generating message
// compressor.
func WithCompressor(cp Compressor) DialOption {
	return func(o *dialOptions) {
		o.cp = cp
	}
}

// WithDecompressor returns a DialOption which sets a DecompressorGenerator for generating
// message decompressor.
func WithDecompressor(dc Decompressor) DialOption {
	return func(o *dialOptions) {
		o.dc = dc
	}
}

// WithBalancer returns a DialOption which sets a load balancer.
func WithBalancer(b Balancer) DialOption {
	return func(o *dialOptions) {
		o.balancer = b
	}
}

// WithServiceConfig returns a DialOption which has a channel to read the service configuration.
func WithServiceConfig(c <-chan ServiceConfig) DialOption {
	return func(o *dialOptions) {
		o.scChan = c
	}
}

// WithBackoffMaxDelay configures the dialer to use the provided maximum delay
// when backing off after failed connection attempts.
func WithBackoffMaxDelay(md time.Duration) DialOption {
	return WithBackoffConfig(BackoffConfig{MaxDelay: md})
}

// WithBackoffConfig configures the dialer to use the provided backoff
// parameters after connection failures.
//
// Use WithBackoffMaxDelay until more parameters on BackoffConfig are opened up
// for use.
func WithBackoffConfig(b BackoffConfig) DialOption {
	// Set defaults to ensure that provided BackoffConfig is valid and
	// unexported fields get default values.
	setDefaults(&b)
	return withBackoff(b)
}

// withBackoff sets the backoff strategy used for retries after a
// failed connection attempt.
//
// This can be exported if arbitrary backoff strategies are allowed by gRPC.
func withBackoff(bs backoffStrategy) DialOption {
	return func(o *dialOptions) {
		o.bs = bs
	}
}

// WithBlock returns a DialOption which makes caller of Dial blocks until the underlying
// connection is up. Without this, Dial returns immediately and connecting the server
// happens in background.
func WithBlock() DialOption {
	return func(o *dialOptions) {
		o.block = true
	}
}

// WithInsecure returns a DialOption which disables transport security for this ClientConn.
// Note that transport security is required unless WithInsecure is set.
// xu:关闭安全检查
func WithInsecure() DialOption {
	return func(o *dialOptions) {
		o.insecure = true
	}
}

// WithTransportCredentials returns a DialOption which configures a
// connection level security credentials (e.g., TLS/SSL).
func WithTransportCredentials(creds credentials.TransportCredentials) DialOption {
	return func(o *dialOptions) {
		o.copts.TransportCredentials = creds
	}
}

// WithPerRPCCredentials returns a DialOption which sets
// credentials which will place auth state on each outbound RPC.
func WithPerRPCCredentials(creds credentials.PerRPCCredentials) DialOption {
	return func(o *dialOptions) {
		o.copts.PerRPCCredentials = append(o.copts.PerRPCCredentials, creds)
	}
}

// WithTimeout returns a DialOption that configures a timeout for dialing a ClientConn
// initially. This is valid if and only if WithBlock() is present.
func WithTimeout(d time.Duration) DialOption {
	return func(o *dialOptions) {
		o.timeout = d
	}
}

// WithDialer returns a DialOption that specifies a function to use for dialing network addresses.
// If FailOnNonTempDialError() is set to true, and an error is returned by f, gRPC checks the error's
// Temporary() method to decide if it should try to reconnect to the network address.
func WithDialer(f func(string, time.Duration) (net.Conn, error)) DialOption {
	return func(o *dialOptions) {
		o.copts.Dialer = func(ctx context.Context, addr string) (net.Conn, error) {
			if deadline, ok := ctx.Deadline(); ok {
				return f(addr, deadline.Sub(time.Now()))
			}
			return f(addr, 0)
		}
	}
}

// WithStatsHandler returns a DialOption that specifies the stats handler
// for all the RPCs and underlying network connections in this ClientConn.
func WithStatsHandler(h stats.Handler) DialOption {
	return func(o *dialOptions) {
		o.copts.StatsHandler = h
	}
}

// FailOnNonTempDialError returns a DialOption that specified if gRPC fails on non-temporary dial errors.
// If f is true, and dialer returns a non-temporary error, gRPC will fail the connection to the network
// address and won't try to reconnect.
// The default value of FailOnNonTempDialError is false.
// This is an EXPERIMENTAL API.
func FailOnNonTempDialError(f bool) DialOption {
	return func(o *dialOptions) {
		o.copts.FailOnNonTempDialError = f
	}
}

// WithUserAgent returns a DialOption that specifies a user agent string for all the RPCs.
func WithUserAgent(s string) DialOption {
	return func(o *dialOptions) {
		o.copts.UserAgent = s
	}
}

// WithUnaryInterceptor returns a DialOption that specifies the interceptor for unary RPCs.
func WithUnaryInterceptor(f UnaryClientInterceptor) DialOption {
	return func(o *dialOptions) {
		o.unaryInt = f
	}
}

// WithStreamInterceptor returns a DialOption that specifies the interceptor for streaming RPCs.
func WithStreamInterceptor(f StreamClientInterceptor) DialOption {
	return func(o *dialOptions) {
		o.streamInt = f
	}
}

// WithAuthority returns a DialOption that specifies the value to be used as
// the :authority pseudo-header. This value only works with WithInsecure and
// has no effect if TransportCredentials are present.
func WithAuthority(a string) DialOption {
	return func(o *dialOptions) {
		o.copts.Authority = a
	}
}

// Dial creates a client connection to the given target.
func Dial(target string, opts ...DialOption) (*ClientConn, error) {
	return DialContext(context.Background(), target, opts...)
}

// DialContext creates a client connection to the given target. ctx can be used to
// cancel or expire the pending connecting. Once this function returns, the
// cancellation and expiration of ctx will be noop. Users should call ClientConn.Close
// to terminate all the pending operations after this function returns.
// This is the EXPERIMENTAL API.
func DialContext(ctx context.Context, target string, opts ...DialOption) (conn *ClientConn, err error) {
	cc := &ClientConn{
		target: target,
		conns:  make(map[Address]*addrConn),
	}
	//这里设置取消的context，可以调用cc.cancel 主动中断dial
	cc.ctx, cc.cancel = context.WithCancel(context.Background())
	for _, opt := range opts {
		opt(&cc.dopts)//初始化所有参数
	}
	if cc.dopts.timeout > 0 {//这个值可以通过 WithTimeout 设置；
		var cancel context.CancelFunc
		//这个方法里面会 在timeout 时间之后，close chan，这样Done() 就可以读了
		ctx, cancel = context.WithTimeout(ctx, cc.dopts.timeout)
		defer cancel() //这一步是用来及时释放资源，里面调用了ctx.cancel,关闭了chan（Done()会返回的那个），
	}

	defer func() {
		select {
		case <-ctx.Done()://收到这个表示 前面WithTimeout设置了超时 并且ctx过期了
			conn, err = nil, ctx.Err()
		default:
		}

		if err != nil {
			cc.Close()
		}
	}()
	//通过WithServiceConfig 设置， 可以异步的 设置service config
	if cc.dopts.scChan != nil {
		// Wait for the initial service config.
		select {
		case sc, ok := <-cc.dopts.scChan:
			if ok {
				cc.sc = sc
			}
		case <-ctx.Done(): //同上
			return nil, ctx.Err()
		}
	}
	// Set defaults.
	if cc.dopts.codec == nil {
		cc.dopts.codec = protoCodec{}//默认编码
	}
	if cc.dopts.bs == nil {
		cc.dopts.bs = DefaultBackoffConfig //默认的backoff重试策略,
	}
	creds := cc.dopts.copts.TransportCredentials
	if creds != nil && creds.Info().ServerName != "" {
		cc.authority = creds.Info().ServerName
	} else if cc.dopts.insecure && cc.dopts.copts.Authority != "" {
		cc.authority = cc.dopts.copts.Authority
	} else {
		colonPos := strings.LastIndex(target, ":")
		if colonPos == -1 {
			colonPos = len(target)
		}
		cc.authority = target[:colonPos]
	}
	var ok bool
	waitC := make(chan error, 1)
	//单独goroutine执行，里面有错误会写到waitC 里， 用来获取集群服务的所有地址，并建立连接
	//虽然go出去了，但是还是要等待这个goroutine执行结束，是阻塞的
	go func() {
		var addrs []Address
		//负载均衡的配置
		//cc.dopts.balancer和cc.sc.LB都是Balancer接口，分别通过WithBalancer 和 通过WithServiceConfig 设置，前者会覆盖后者
		if cc.dopts.balancer == nil && cc.sc.LB != nil {
			cc.dopts.balancer = cc.sc.LB
		}
		if cc.dopts.balancer == nil {
			// Connect to target directly if balancer is nil.
			//如果没有设置balancer，地址列表里面就只有target这一个地址
			addrs = append(addrs, Address{Addr: target})
		} else {
			var credsClone credentials.TransportCredentials
			if creds != nil {
				credsClone = creds.Clone()
			}
			config := BalancerConfig{
				DialCreds: credsClone,
			}
			//balancer(etcd等)的初始化
			if err := cc.dopts.balancer.Start(target, config); err != nil {
				waitC <- err
				return
			}
			//这里会 返回一个chan，元素是每一次地址更新后的所有地址（是全量，不是增量）
			ch := cc.dopts.balancer.Notify()
			if ch == nil {
				// There is no name resolver installed.
				addrs = append(addrs, Address{Addr: target})
			} else {
				addrs, ok = <-ch//ok 表示chan是否关闭，如果关闭了就不需要lbWatcher了（监控地址改动）
				if !ok || len(addrs) == 0 {
					waitC <- errNoAddr //没有从balance找到有效的地址
					return
				}
			}
		}
		//对于每一个地址 建立连接;
		// 如果调用了WithBlock，则这一步是阻塞的，一直等到所有连接成功（注：建议不要这样，除非你知道你在干什么）
		// 否则里面是通过goroutine异步处理的，不会等待所有的连接成功
		for _, a := range addrs {
			if err := cc.resetAddrConn(a, false, nil); err != nil {
				waitC <- err
				return
			}
		}
		close(waitC) //关闭waitC，这样会读取到err=nil
	}()
	select {
	case <-ctx.Done()://同上
		return nil, ctx.Err()
	case err := <-waitC://这一步会等待上一个goroutine结束
		if err != nil {
			return nil, err
		}
	}

	// If balancer is nil or balancer.Notify() is nil, ok will be false here.
	// The lbWatcher goroutine will not be created.
	if ok {//只有采用balancer（etcd等）的才会走到这一步，监控服务集群地址的变化
		go cc.lbWatcher()
	}

	if cc.dopts.scChan != nil {
		go cc.scWatcher()//监控ServiceConfig的变化，这样可以在dial之后动态的修改client的访问服务的配置ServiceConfig
	}
	return cc, nil
}

// ConnectivityState indicates the state of a client connection.
type ConnectivityState int

const (
	// Idle indicates the ClientConn is idle.
	Idle ConnectivityState = iota
	// Connecting indicates the ClienConn is connecting.
	Connecting
	// Ready indicates the ClientConn is ready for work.
	Ready
	// TransientFailure indicates the ClientConn has seen a failure but expects to recover.
	TransientFailure
	// Shutdown indicates the ClientConn has started shutting down.
	Shutdown
)

func (s ConnectivityState) String() string {
	switch s {
	case Idle:
		return "IDLE"
	case Connecting:
		return "CONNECTING"
	case Ready:
		return "READY"
	case TransientFailure:
		return "TRANSIENT_FAILURE"
	case Shutdown:
		return "SHUTDOWN"
	default:
		panic(fmt.Sprintf("unknown connectivity state: %d", s))
	}
}

// ClientConn represents a client connection to an RPC server.
type ClientConn struct {
	ctx    context.Context//context.WithCancel dial的时候是这个，
	cancel context.CancelFunc// 上面的context.WithCancel 返回的，可以主动调用cancel，断开dial

	target    string //若果在没有负载均衡的情况下，这个就是服务器的地址（ip:port）; 有负载均衡的情况下是etcd等的key（通过这个key）
	authority string
	dopts     dialOptions //建立连接的各项配置

	mu    sync.RWMutex
	sc    ServiceConfig
	conns map[Address]*addrConn// 维护各个地址对应的连接，每一个addrConn 维护一个长连接
}

func (cc *ClientConn) lbWatcher() {
	//Notify 返回的是一个chan，元素是每一次地址更新后的所有地址（是全量，不是增量）
	for addrs := range cc.dopts.balancer.Notify() {
		var (
			add []Address   // Addresses need to setup connections.
			del []*addrConn // Connections need to tear down.
		)
		cc.mu.Lock()//地址存在cc.conns，操作需要加锁
		for _, a := range addrs {
			if _, ok := cc.conns[a]; !ok {
				add = append(add, a)//新增的服务地址（可以认为集群新增了机器/服务）
			}
		}
		for k, c := range cc.conns {
			var keep bool
			for _, a := range addrs {
				if k == a {
					keep = true
					break
				}
			}
			if !keep {
				del = append(del, c)//删除的地址（可以认为这个地址从blancer中去掉了里面删了的服务挂了，所以要踢出去）
				delete(cc.conns, c.addr)
			}
		}
		cc.mu.Unlock()
		for _, a := range add {
			cc.resetAddrConn(a, true, nil)//新增的地址建立连接
		}
		for _, c := range del {
			c.tearDown(errConnDrain)//关闭这个地址的连接
		}
	}
}

func (cc *ClientConn) scWatcher() {
	for {
		select {
		case sc, ok := <-cc.dopts.scChan:
			if !ok {
				return
			}
			cc.mu.Lock()
			// TODO: load balance policy runtime change is ignored.
			// We may revist this decision in the future.
			cc.sc = sc
			cc.mu.Unlock()
		case <-cc.ctx.Done():
			return
		}
	}
}

// resetAddrConn creates an addrConn for addr and adds it to cc.conns.
// If there is an old addrConn for addr, it will be torn down, using tearDownErr as the reason.
// If tearDownErr is nil, errConnDrain will be used instead.
func (cc *ClientConn) resetAddrConn(addr Address, skipWait bool, tearDownErr error) error {
	ac := &addrConn{
		cc:    cc,
		addr:  addr,
		dopts: cc.dopts,
	}
	ac.ctx, ac.cancel = context.WithCancel(cc.ctx)
	ac.stateCV = sync.NewCond(&ac.mu)
	if EnableTracing {
		ac.events = trace.NewEventLog("grpc.ClientConn", ac.addr.Addr)
	}
	if !ac.dopts.insecure {
		if ac.dopts.copts.TransportCredentials == nil {
			return errNoTransportSecurity
		}
	} else {
		if ac.dopts.copts.TransportCredentials != nil {
			return errCredentialsConflict
		}
		for _, cd := range ac.dopts.copts.PerRPCCredentials {
			if cd.RequireTransportSecurity() {
				return errTransportCredentialsMissing
			}
		}
	}
	// Track ac in cc. This needs to be done before any getTransport(...) is called.
	cc.mu.Lock()
	if cc.conns == nil {
		cc.mu.Unlock()
		return ErrClientConnClosing
	}
	stale := cc.conns[ac.addr] //获取旧的addrConn，可能没有，就是nil
	cc.conns[ac.addr] = ac //用新的addrConn 替换
	cc.mu.Unlock()
	if stale != nil {
		//已经存在一个旧的addrConn，需要关闭，有两种可能
		//1. balancer(etcd等) 存在bug，返回了重复的地址~~O(∩_∩)O哈哈~完美甩锅
		//2. 旧的ac收到http2 的goaway（表示服务端不接受新的请求了，但是已有的请求要继续处理完），这里又新建一个，？~在transportMonitor里
		// There is an addrConn alive on ac.addr already. This could be due to
		// 1) a buggy Balancer notifies duplicated Addresses;
		// 2) goaway was received, a new ac will replace the old ac.
		//    The old ac should be deleted from cc.conns, but the
		//    underlying transport should drain rather than close.
		if tearDownErr == nil {
			// tearDownErr is nil if resetAddrConn is called by
			// 1) Dial
			// 2) lbWatcher
			// In both cases, the stale ac should drain, not close.
			stale.tearDown(errConnDrain)
		} else {
			stale.tearDown(tearDownErr)
		}
	}
	//通过WithBlock设置为true后，这里会阻塞，直到所有连接成功；
	// skipWait may overwrite the decision in ac.dopts.block.
	if ac.dopts.block && !skipWait {
		if err := ac.resetTransport(false); err != nil {
			if err != errConnClosing { //如果有错 且不是errConnClosing(表示已经关闭)
				// Tear down ac and delete it from cc.conns.
				cc.mu.Lock()
				delete(cc.conns, ac.addr) //从cc.conns 删除，不在维护
				cc.mu.Unlock()
				ac.tearDown(err)// 关闭
			}
			if e, ok := err.(transport.ConnectionError); ok && !e.Temporary() {
				return e.Origin()
			}
			return err
		}
		// Start to monitor the error status of transport.
		go ac.transportMonitor()
	} else {//这里不会阻塞，异步的建立连接
		// Start a goroutine connecting to the server asynchronously.
		go func() {
			if err := ac.resetTransport(false); err != nil {
				grpclog.Printf("Failed to dial %s: %v; please retry.", ac.addr.Addr, err)
				if err != errConnClosing {
					// Keep this ac in cc.conns, to get the reason it's torn down.
					ac.tearDown(err) //关闭，（和上面相比）但是不从cc.conns 删除，为了方便得到tearDownErr reason
				}
				return
			}
			ac.transportMonitor()
		}()
	}
	return nil
}

// TODO: Avoid the locking here.
func (cc *ClientConn) getMethodConfig(method string) (m MethodConfig, ok bool) {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	m, ok = cc.sc.Methods[method]
	return
}

func (cc *ClientConn) getTransport(ctx context.Context, opts BalancerGetOptions) (transport.ClientTransport, func(), error) {
	var (
		ac  *addrConn
		ok  bool
		put func()
	)
	if cc.dopts.balancer == nil { //如果没有设置balancer，只会有一个地址，直接返回
		// If balancer is nil, there should be only one addrConn available.
		cc.mu.RLock()
		if cc.conns == nil {
			cc.mu.RUnlock()
			return nil, nil, toRPCErr(ErrClientConnClosing)
		}
		for _, ac = range cc.conns {
			// Break after the first iteration to get the first addrConn.
			ok = true
			break
		}
		cc.mu.RUnlock()
	} else {//如果有设置balancer（etcd等），根据策略选一个（默认是轮询）
		var (
			addr Address
			err  error
		)
		//得到一个地址，如果!BlockingWait即failfast（默认），不保证这个地址一定是有效的；反之，则能保证
		addr, put, err = cc.dopts.balancer.Get(ctx, opts)//(rr *roundRobin) Get
		if err != nil {
			return nil, nil, toRPCErr(err)
		}
		cc.mu.RLock()
		if cc.conns == nil {
			cc.mu.RUnlock()
			return nil, nil, toRPCErr(ErrClientConnClosing)
		}
		ac, ok = cc.conns[addr]
		cc.mu.RUnlock()
	}
	if !ok { //如果这个地址不在cc.conns里，说明这个地址已经被删了(比如etcd中掉了)
		if put != nil {
			put()
		}
		return nil, nil, errConnClosing
	}
	//等待一个连接，默认情况下，这里是保证连接ok，如果不ok会返回错误
	t, err := ac.wait(ctx, cc.dopts.balancer != nil, !opts.BlockingWait)
	if err != nil {
		if put != nil {
			put()
		}
		return nil, nil, err
	}
	return t, put, nil
}

// Close tears down the ClientConn and all underlying connections.
func (cc *ClientConn) Close() error {
	cc.cancel()

	cc.mu.Lock()
	if cc.conns == nil {
		cc.mu.Unlock()
		return ErrClientConnClosing
	}
	conns := cc.conns
	cc.conns = nil
	cc.mu.Unlock()
	if cc.dopts.balancer != nil {
		cc.dopts.balancer.Close()
	}
	for _, ac := range conns {
		ac.tearDown(ErrClientConnClosing)
	}
	return nil
}

// addrConn is a network connection to a given address.
type addrConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	cc     *ClientConn
	addr   Address
	dopts  dialOptions
	events trace.EventLog

	mu      sync.Mutex
	state   ConnectivityState
	stateCV *sync.Cond
	down    func(error) // the handler called when a connection is down.
	// ready is closed and becomes nil when a new transport is up or failed
	// due to timeout.
	ready     chan struct{}
	transport transport.ClientTransport

	// The reason this addrConn is torn down.
	tearDownErr error
}

// printf records an event in ac's event log, unless ac has been closed.
// REQUIRES ac.mu is held.
func (ac *addrConn) printf(format string, a ...interface{}) {
	if ac.events != nil {
		ac.events.Printf(format, a...)
	}
}

// errorf records an error in ac's event log, unless ac has been closed.
// REQUIRES ac.mu is held.
func (ac *addrConn) errorf(format string, a ...interface{}) {
	if ac.events != nil {
		ac.events.Errorf(format, a...)
	}
}

// getState returns the connectivity state of the Conn
func (ac *addrConn) getState() ConnectivityState {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	return ac.state
}

// waitForStateChange blocks until the state changes to something other than the sourceState.
func (ac *addrConn) waitForStateChange(ctx context.Context, sourceState ConnectivityState) (ConnectivityState, error) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if sourceState != ac.state {
		return ac.state, nil
	}
	done := make(chan struct{})
	var err error
	go func() {
		select {
		case <-ctx.Done():
			ac.mu.Lock()
			err = ctx.Err()
			ac.stateCV.Broadcast()
			ac.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)
	for sourceState == ac.state {
		ac.stateCV.Wait()
		if err != nil {
			return ac.state, err
		}
	}
	return ac.state, nil
}

func (ac *addrConn) resetTransport(closeTransport bool) error {
	for retries := 0; ; retries++ { //一直循重试建立连接直到成功，除非某些条件下返回
		ac.mu.Lock()
		ac.printf("connecting")
		if ac.state == Shutdown {
			//Shutdown状态表示这个连接已经关闭，不需要维护了，通常服务先挂了，然后balancer(etcd等)中这个地址又被移除了的情况会走到这，直接返回
			// ac.tearDown(...) has been invoked.
			ac.mu.Unlock()
			return errConnClosing
		}
		if ac.down != nil {
			//一般通过存balancer（etcd等）的UP 方法返回的，
			// 如果存在，这里就通知balancer 这个地址连接不ok了，不要用了，其实是因为接下里要reset，O(∩_∩)O哈哈~
			ac.down(downErrorf(false, true, "%v", errNetworkIO))
			ac.down = nil
		}
		ac.state = Connecting //状态改为正在连接中~~
		ac.stateCV.Broadcast()//lzy todo
		t := ac.transport
		ac.mu.Unlock()
		if closeTransport && t != nil { //旧的要关闭
			t.Close()
		}
		//这里是根据重试的超时策略，返回两次重试的间隔时间；即如果这次重连还是失败，会等待sleepTime才会进入下一次循环
		//retries越大，sleepTime越大
		sleepTime := ac.dopts.bs.backoff(retries)
		timeout := minConnectTimeout
		if timeout < sleepTime {
			timeout = sleepTime
		}
		//设置超时时间，最小20s
		//注：为啥下面err==nil的时候cancel没有执行（官方推荐的是立马defer cancel()） bug？？？？？
		//已经提了个issue https://github.com/grpc/grpc-go/issues/1099
		ctx, cancel := context.WithTimeout(ac.ctx, timeout)
		connectTime := time.Now()
		sinfo := transport.TargetInfo{
			Addr:     ac.addr.Addr,
			Metadata: ac.addr.Metadata,
		}
		newTransport, err := transport.NewClientTransport(ctx, sinfo, ac.dopts.copts)
		if err != nil {
			cancel()
			//如果不是临时错误，立马返回；否则接下里会重试
			if e, ok := err.(transport.ConnectionError); ok && !e.Temporary() {
				return err
			}
			grpclog.Printf("grpc: addrConn.resetTransport failed to create client transport: %v; Reconnecting to %v", err, ac.addr)
			ac.mu.Lock()
			if ac.state == Shutdown {//同上，即这个地址不需要了
				// ac.tearDown(...) has been invoked.
				ac.mu.Unlock()
				return errConnClosing
			}
			ac.errorf("transient failure: %v", err)
			ac.state = TransientFailure //状态改为短暂的失败
			ac.stateCV.Broadcast()
			if ac.ready != nil {//只有进入ac.wait()才会走入这个逻辑，表示有一个请求正在等待这个地址的连接是成功还是失败
				close(ac.ready) //建立连接失败了 关闭ready
				ac.ready = nil
			}
			ac.mu.Unlock()
			closeTransport = false
			select {
			case <-time.After(sleepTime - time.Since(connectTime))://一直等待足够sleepTime长时间，再进入下一次循环
			case <-ac.ctx.Done()://这个连接是被cancel掉（超时或者主动cancel）
				return ac.ctx.Err()
			}
			continue
		}
		ac.mu.Lock()
		ac.printf("ready")
		if ac.state == Shutdown {//同上，所以这个时候要把已经建立的连接close，手动从etcd中删除这个地址会走到这
			// ac.tearDown(...) has been invoked.
			ac.mu.Unlock()
			newTransport.Close()
			return errConnClosing
		}
		ac.state = Ready //状态ok了
		ac.stateCV.Broadcast()
		ac.transport = newTransport
		if ac.ready != nil {//只有进入ac.wait()才会走入这个逻辑，表示有一个请求正在等待这个地址的连接是成功还是失败
			close(ac.ready) //建立连接成功了关闭ready
			ac.ready = nil
		}
		//如果存在balancer（etcd等）就通知balancer 这个地址连接ok了，可以用了
		if ac.cc.dopts.balancer != nil {
			ac.down = ac.cc.dopts.balancer.Up(ac.addr)
		}
		ac.mu.Unlock()
		return nil
	}
}

// Run in a goroutine to track the error in transport and create the
// new transport if an error happens. It returns when the channel is closing.
func (ac *addrConn) transportMonitor() {
	for {
		ac.mu.Lock()
		t := ac.transport
		ac.mu.Unlock()
		select {
		// This is needed to detect the teardown when
		// the addrConn is idle (i.e., no RPC in flight).
		case <-ac.ctx.Done(): //这一步 表示ac.teardown 了，不需要维护了，return掉
			select {
			case <-t.Error():
				t.Close()
			default:
			}
			return
		case <-t.GoAway():// 这一步，会resetAddrConn，里面会新建一个transportMonitor，这里就不需要维护了，retrrn掉
			// If GoAway happens without any network I/O error, ac is closed without shutting down the
			// underlying transport (the transport will be closed when all the pending RPCs finished or
			// failed.).
			// If GoAway and some network I/O error happen concurrently, ac and its underlying transport
			// are closed.
			// In both cases, a new ac is created.
			select {
			case <-t.Error():
				ac.cc.resetAddrConn(ac.addr, true, errNetworkIO)
			default:
				ac.cc.resetAddrConn(ac.addr, true, errConnDrain)
			}
			return
		case <-t.Error()://这里是transport 有错，
			select {
			case <-ac.ctx.Done()://这一步 表示ac.teardown（比如ctx 被cancel的情况）不需要维护了，return掉
				t.Close()
				return
			case <-t.GoAway()://同上
				ac.cc.resetAddrConn(ac.addr, true, errNetworkIO)
				return
			default: //如果有错，走到这里~~没有return， 下面会重连
			}
			ac.mu.Lock()
			if ac.state == Shutdown { //不是通过ac.teardown的（这种情况会走到上面），但是还Shutdown的了，什么情况呢？？？
				// ac has been shutdown.
				ac.mu.Unlock()
				return
			}
			ac.state = TransientFailure //置为临时失败，暂时不让用
			ac.stateCV.Broadcast()
			ac.mu.Unlock()
			if err := ac.resetTransport(true); err != nil {
				ac.mu.Lock()
				ac.printf("transport exiting: %v", err)
				ac.mu.Unlock()
				grpclog.Printf("grpc: addrConn.transportMonitor exits due to: %v", err)
				if err != errConnClosing {
					// Keep this ac in cc.conns, to get the reason it's torn down.
					ac.tearDown(err)
				}
				return
			}
		}
	}
}
//等待failfast默认是true）
//默认情况下 ac.state=Shutdown，ready，TransientFailure，直接返回
//否则，ac.state=Connecting等，则等待ready(成功或者失败),直到ctx超时
// wait blocks until i) the new transport is up or ii) ctx is done or iii) ac is closed or
// iv) transport is in TransientFailure and there is a balancer/failfast is true.
func (ac *addrConn) wait(ctx context.Context, hasBalancer, failfast bool) (transport.ClientTransport, error) {
	for {
		ac.mu.Lock()
		switch {
		case ac.state == Shutdown:
			if failfast || !hasBalancer {
				// RPC is failfast or balancer is nil. This RPC should fail with ac.tearDownErr.
				err := ac.tearDownErr
				ac.mu.Unlock()
				return nil, err
			}
			ac.mu.Unlock()
			return nil, errConnClosing
		case ac.state == Ready:
			ct := ac.transport
			ac.mu.Unlock()
			return ct, nil
		case ac.state == TransientFailure:
			if failfast || hasBalancer {
				ac.mu.Unlock()
				return nil, errConnUnavailable
			}
		}
		//ac.state=Connecting,默认情况下，走到这里，表示正在连接~~，所以等一等~~
		ready := ac.ready
		if ready == nil {
			ready = make(chan struct{})
			ac.ready = ready
		}
		ac.mu.Unlock()
		select {
		case <-ctx.Done()://这个请求被cancel了（超时等等）
			return nil, toRPCErr(ctx.Err())
		// Wait until the new transport is ready or failed.
		//不管成功还是失败，都会close(ac.ready)
		case <-ready:
		}
	}
}

// tearDown starts to tear down the addrConn.
// TODO(zhaoq): Make this synchronous to avoid unbounded memory consumption in
// some edge cases (e.g., the caller opens and closes many addrConn's in a
// tight loop.
// tearDown doesn't remove ac from ac.cc.conns.
func (ac *addrConn) tearDown(err error) {
	ac.cancel() //执行这个之后ac.ctx.Done() 会收到消息

	ac.mu.Lock()
	defer ac.mu.Unlock()
	if ac.down != nil {//通知balancer 这个地址 down了
		ac.down(downErrorf(false, false, "%v", err))
		ac.down = nil
	}
	if err == errConnDrain && ac.transport != nil {
		// GracefulClose(...) may be executed multiple times when
		// i) receiving multiple GoAway frames from the server; or
		// ii) there are concurrent name resolver/Balancer triggered
		// address removal and GoAway.
		ac.transport.GracefulClose()
	}
	if ac.state == Shutdown {
		return
	}
	ac.state = Shutdown
	ac.tearDownErr = err
	ac.stateCV.Broadcast()
	if ac.events != nil {
		ac.events.Finish()
		ac.events = nil
	}
	if ac.ready != nil {
		close(ac.ready)
		ac.ready = nil
	}
	if ac.transport != nil && err != errConnDrain {
		ac.transport.Close()
	}
	return
}
