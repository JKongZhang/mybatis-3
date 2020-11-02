package me.labazhang.mybatis;

import org.apache.ibatis.annotations.Mapper;

/**
 * 用户操作Mapper
 *
 * @author JKong
 * @version v0.0.1
 * @date 2020/6/24 12:45.
 */
@Mapper
public interface UserMapper {

    /**
     * 根据ID获取用户信息
     *
     * @param id 用户Id
     * @return 用户信息
     */
    User findUserById(Integer id);
}