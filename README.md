#gRPC-Go

源码注解

基于34384f34de585705f1a6783a158d2ec8af29f618

切换到note分支
建议使用ide的代码提示和跳转等功能阅读, 所以目录结构要正确
可以：
```
cd $GOPATH/google.golang.org/
git clone https://github.com/liangzhiyang/annotate-grpc-go.git
mv annotate-grpc-go grpc
```
如果 grpc 已经存在
```
cd $GOPATH/google.golang.org/grpc
git remote add lzy  https://github.com/liangzhiyang/annotate-grpc-go.git
git fetch --all
git checkout -b note lzy/note
```
建议阅读顺序(细节不列)
* grpc.Dial() //建立连接的过程
```
(cc *ClientConn) resetAddrConn
(ac *addrConn) resetTransport
(ac *addrConn) transportMonitor //单独的goroutine，管理transport
transport.newHTTP2Client
(t *http2Client) reader() //单独的goroutine，读取服务端的帧数据
```

* grpc.Invoke() //一次rpc请求的过程

```
(cc *ClientConn) getTransport
(ac *addrConn) wait
sendRequest()
recvResponse()

```

持续更新~~~~

准备接下来列举几种情况说明client端遇到意外情况的代码执行流程（使用balancer）

[grpc源码注解(golang)](http://blog.csdn.net/liangzhiyang/article/details/60963025)

[grpc的dial正常执行流程](http://blog.csdn.net/liangzhiyang/article/details/61921764)

[grpc服务异常情况的执行流程](http://blog.csdn.net/liangzhiyang/article/details/61921843)

[grpc的invoke(一次请求)正常执行流程](http://blog.csdn.net/liangzhiyang/article/details/62230971)




Golang gRPC实践：https://github.com/Jergoo/go-grpc-example
