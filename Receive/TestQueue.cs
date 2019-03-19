using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Receive
{
    public class TestQueue
    {
        private static AutoResetEvent myEvent = new AutoResetEvent(false);
        private RabbitMQHelper rabbit = new RabbitMQHelper();
        private ushort nChannel = 10;//一个连接的最大通道数和所开的线程数一致

        public void Timer()
        {
            System.Timers.Timer t = new System.Timers.Timer(20000);//实例化Timer类，设置间隔时间为10000毫秒；
            t.Elapsed += new System.Timers.ElapsedEventHandler(TiemrCheck);//到达时间的时候执行事件；
            t.AutoReset = true;//设置是执行一次（false）还是一直执行(true)；
            t.Enabled = true;//是否执行System.Timers.Timer.Elapsed事件；
        }

        private void TiemrCheck(object source, System.Timers.ElapsedEventArgs e)
        {
            if (!rabbit.IsOpen())//定时检测RabbitMQ连接是否打开，若没打开，则开线程打开连接
            {
                Thread thread1 = new Thread(CreateConnecttion);
                thread1.IsBackground = true;
                thread1.Start();
            }
        }

        /// <summary>
        /// 单个RabbitMQ连接开多个线程，每个线程开一个channel接受消息
        /// </summary>
        private void CreateConnecttion()
        {
            try
            {
                rabbit.RabbitConnection("localhost", nChannel);
                if (rabbit.conn != null)
                {
                    ThreadPool.SetMinThreads(1, 1);
                    ThreadPool.SetMaxThreads(100, 100);
                    for (int i = 1; i <= nChannel; i++)
                    {
                        ThreadPool.QueueUserWorkItem(new WaitCallback(ReceiveMsg), "");
                    }
                    myEvent.WaitOne();//等待所有线程工作完成后，才能关闭连接
                    rabbit.Close();
                }
            }
            catch (Exception ex)
            {
                rabbit.Close();
                Console.WriteLine(ex.Message);
            }
        }

        /// <summary>
        /// 接收并处理消息，在一个连接中创建多个通道（channel），避免创建多个连接
        /// </summary>
        /// <param name="con">RabbitMQ连接</param>
        private void ReceiveMsg(object obj)
        {
            IModel channel = null;
            try
            {
                #region 创建评阅通道，定义中转站和队列
                channel = rabbit.conn.CreateModel();
                channel.ExchangeDeclare(
                    exchange: "TestTopicChange", //exchange名称
                    type: ExchangeType.Topic, //Topic模式，采用路由匹配
                    durable: true,//exchange持久化
                    autoDelete: false,//是否自动删除，一般设成false
                    arguments: null//一些结构化参数，比如:alternate-exchange
                    );

                //定义阅卷队列
                channel.QueueDeclare(
                    queue: "Test_Queue", //队列名称
                    durable: true, //队列磁盘持久化(要和消息持久化一起使用才有效)
                    exclusive: false,//是否排他的，false。如果一个队列声明为排他队列，该队列首次声明它的连接可见，并在连接断开时自动删除
                    autoDelete: false,
                    arguments: null
                    );
                #endregion
                channel.BasicQos(0, 1, false);//每次只接收一条消息

                channel.QueueBind(queue: "Test_Queue",
                                      exchange: "TestTopicChange",
                                      routingKey: "TestRouteKey.*");
                var consumer = new EventingBasicConsumer(channel);

                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;
                    //处理消息方法
                    try
                    {
                        bool isMark = AutoMark(message);
                        if (isMark)
                        {
                            //Function.writeMarkLog(message);
                            //确认该消息已被消费,发消息给RabbitMQ队列
                            channel.BasicAck(ea.DeliveryTag, false);
                        }
                        else
                        {
                            if (MarkErrorSend(message))//把错误消息推到评阅错误消息队列
                                channel.BasicReject(ea.DeliveryTag, false);
                            else
                                //消费消息失败，拒绝此消息，重回队列，让它可以继续发送到其他消费者 
                                channel.BasicReject(ea.DeliveryTag, true);
                        }
                    }
                    catch (Exception ex)
                    {
                        try
                        {
                            Console.WriteLine(ex.Message);
                            if (channel != null && channel.IsOpen)//处理RabbitMQ停止重启而自动评阅崩溃的问题
                            {
                                //消费消息失败，拒绝此消息，重回队列，让它可以继续发送到其他消费者 
                                channel.BasicReject(ea.DeliveryTag, true);
                            }
                        }
                        catch { }
                    }
                };
                //手动确认消息
                channel.BasicConsume(queue: "Test_Queue",
                                     autoAck: false,
                                     consumer: consumer);
            }
            catch (Exception ex)
            {
                try
                {
                    Console.WriteLine("接收消息方法出错：" + ex.Message);
                    if (channel != null && channel.IsOpen)//关闭通道
                        channel.Close();
                    if (rabbit.conn != null)//处理RabbitMQ突然停止的问题
                        rabbit.Close();
                }
                catch { }
            }
        }

        /// <summary>
        /// 处理消息
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        private bool AutoMark(string msg)
        {
            Console.WriteLine(msg);
            return true;
        }

        /// <summary>
        /// 把处理错误的消息发送到“错误消息队列”
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        private bool MarkErrorSend(string msg)
        {
            RabbitMQHelper MQ = new RabbitMQHelper();
            MQ.RabbitConnection("localhost",1);
            //创建通道
            var channel = MQ.conn.CreateModel();
            try
            {
                //定义一个Direct类型交换机
                channel.ExchangeDeclare(
                    exchange: "ErrorTopicChange", //exchange名称
                    type: ExchangeType.Topic, //Topic模式，采用路由匹配
                    durable: true,//exchange持久化
                    autoDelete: false,//是否自动删除，一般设成false
                    arguments: null//一些结构化参数，比如:alternate-exchange
                    );

                //定义阅卷队列
                channel.QueueDeclare(
                    queue: "Error_Queue", //队列名称
                    durable: true, //队列磁盘持久化(要和消息持久化一起使用才有效)
                    exclusive: false,//是否排他的，false。如果一个队列声明为排他队列，该队列首次声明它的连接可见，并在连接断开时自动删除
                    autoDelete: false,//是否自动删除，一般设成false
                    arguments: null
                    );

                //将队列绑定到交换机
                string routeKey = "ErrorRouteKey.*";//*匹配一个单词
                channel.QueueBind(
                    queue: "Error_Queue",
                    exchange: "ErrorTopicChange",
                    routingKey: routeKey,
                    arguments: null
                    );

                //消息磁盘持久化，把DeliveryMode设成2（要和队列持久化一起使用才有效）
                IBasicProperties properties = channel.CreateBasicProperties();
                properties.DeliveryMode = 2;

                channel.ConfirmSelect();//发送确认机制，消息过多可考虑分几个通道（channel）来发
                byte[] sendBytes = Encoding.UTF8.GetBytes(msg);
                //发布消息
                channel.BasicPublish(
                    exchange: "ErrorTopicChange",
                    routingKey: "ErrorRouteKey.one",
                    basicProperties: properties,
                    body: sendBytes
                    );

                bool isAllPublished = channel.WaitForConfirms();//通道（channel）里所有消息均发送才返回true
                return isAllPublished;
            }
            catch (Exception ex)
            {
                //写错误日志
                return false;
            }
            finally
            {
                channel.Close();
                MQ.conn.Close();
            }
        }

    }
}
