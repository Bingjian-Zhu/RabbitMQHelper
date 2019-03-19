using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Receive
{
    public class RabbitMQHelper
    {
        public IConnection conn = null;

        /// <summary>
        /// 创建RabbitMQ消息中间件连接
        /// </summary>
        /// <returns>返回连接对象</returns>
        public IConnection RabbitConnection(string sHostName, ushort nChannelMax)
        {
            try
            {
                if (conn == null)
                {
                    var factory = new ConnectionFactory()
                    {
                        UserName = "guest",//用户名
                        Password = "guest",//密码
                        HostName = sHostName,//ConfigurationManager.AppSettings["MQIP"],
                        AutomaticRecoveryEnabled = false,//取消自动重连，改用定时器定时检测连接是否存在
                        RequestedConnectionTimeout = 10000,//请求超时时间设成10秒，默认的为30秒
                        RequestedChannelMax = nChannelMax//与开的线程数保持一致
                    };
                    //创建连接
                    conn = factory.CreateConnection();
                    Console.WriteLine("RabbitMQ连接已创建！");
                }

                return conn;
            }
            catch
            {
                Console.WriteLine("创建连接失败，请检查RabbitMQ是否正常运行！");
                return null;
            }
        }

        /// <summary>
        /// 关闭RabbitMQ连接
        /// </summary>
        public void Close()
        {
            try
            {
                if (conn != null)
                {
                    if (conn.IsOpen)
                        conn.Close();
                    conn = null;
                    Console.WriteLine("RabbitMQ连接已关闭！");
                }
            }
            catch { }
        }

        /// <summary>
        /// 判断RabbitMQ连接是否打开
        /// </summary>
        /// <returns></returns>
        public bool IsOpen()
        {
            try
            {
                if (conn != null)
                {
                    if (conn.IsOpen)
                        return true;
                }
                return false;
            }
            catch
            {
                return false;
            }
        }
    }
}
