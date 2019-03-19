using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Send
{
    class Program
    {
        static void Main(string[] args)
        {
            //消息数组
            string[] lstMessage = new string[100];
            for(int i = 0; i < 100; i++)
            {
                lstMessage[i] = i.ToString();
            }
            bool res = MarkErrorSend(lstMessage);
        }

        private static bool MarkErrorSend(string[] lstMsg)
        {
            try
            {
                var factory = new ConnectionFactory()
                {
                    UserName = "guest",//用户名
                    Password = "guest",//密码
                    HostName = "localhost",//ConfigurationManager.AppSettings["sHostName"],
                };
                //创建连接
                var connection = factory.CreateConnection();
                //创建通道
                var channel = connection.CreateModel();
                try
                {
                    //定义一个Direct类型交换机
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
                        autoDelete: false,//是否自动删除，一般设成false
                        arguments: null
                        );

                    //将队列绑定到交换机
                    string routeKey = "TestRouteKey.*";//*匹配一个单词
                    channel.QueueBind(
                        queue: "Test_Queue",
                        exchange: "TestTopicChange",
                        routingKey: routeKey,
                        arguments: null
                        );

                    //消息磁盘持久化，把DeliveryMode设成2（要和队列持久化一起使用才有效）
                    IBasicProperties properties = channel.CreateBasicProperties();
                    properties.DeliveryMode = 2;

                    channel.ConfirmSelect();//发送确认机制
                    foreach (var itemMsg in lstMsg)
                    {
                        byte[] sendBytes = Encoding.UTF8.GetBytes(itemMsg);
                        //发布消息
                        channel.BasicPublish(
                            exchange: "TestTopicChange",
                            routingKey: "TestRouteKey.one",
                            basicProperties: properties,
                            body: sendBytes
                            );
                    }
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
                    connection.Close();
                }
            }
            catch
            {
                //RabbitMQ.Client.Exceptions.BrokerUnreachableException:
                //When the configured hostname was not reachable.
                return false;
            }
        }
    }
}
