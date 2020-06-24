package me.jkong.mybatis;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

/**
 * 使用 statementId 获取SQL并执行SQL获取结果
 *
 * @author JKong
 * @version v0.0.1
 * @date 2020/6/24 12:39.
 */
public class SimpleMyBatisTest {

    public static void main(String[] args) {
        InputStream in = null;
        SqlSession session = null;
        try {
            //1.读取配置文件
            in = Resources.getResourceAsStream("resources/mybatis.xml");
            //2.创建SqlSessionFactory工厂
            SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(in);
            //3.使用工厂生产SqlSession对象
            session = sqlSessionFactory.openSession();
            //4.执行Sql语句
            User user = session.selectOne("me.jkong.mybatis.UserMapper.findUserById", 1);
            //5. 打印结果
            System.out.println(user);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //6.释放资源
            if (session != null) {
                session.close();
            }
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignore) {
                    // ignore
                }
            }
        }


    }

}
