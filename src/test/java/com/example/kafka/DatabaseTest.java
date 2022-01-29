package com.example.kafka;

import com.example.kafka.dao.AsPerson;
import com.example.kafka.dao.KafkaOffset;
import com.example.kafka.mapper.AsPersonMapper;
import com.example.kafka.mapper.KafkaOffsetMapper;
import com.example.kafka.mapper.MyBaseMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class DatabaseTest {

    @Resource
    private KafkaOffsetMapper kafkaOffsetMapper;
    @Resource
    protected AsPersonMapper asPersonMapper;
    @Resource
    private MyBaseMapper myBaseMapper;

    @Test
    public void testSelect() {
        System.out.println(("----- selectAll method test ------"));
        List<AsPerson> userList = asPersonMapper.selectList(null);
        userList.forEach(System.out::println);
    }

    /**
     * 测试插入
     * Id 生成策略 雪花算法
     * 使用 4bit 作为毫秒数
     * 10bit 作为机器ID
     * 12bit 作为毫秒内的流水号(每个节点在每毫秒可以产生 4096个ID)
     * 最后一位数永远是0
     */
    @Test
    public void insertTest() {
        AsPerson asPerson = new AsPerson();
        asPerson.setIid(new Date().getTime() + "");
        asPerson.setCusecode("new-person1");
        asPerson.setCusepassword(new Date().getTime() + "");

        //插入时mybatis-plus 会自动生成ID 写入 表中的id 字段
        int result = asPersonMapper.insert(asPerson);
        //结果受影响的行数
        System.out.println(result);
    }

    /**
     * 测试更新
     * 阿里巴巴开发手册规定 每个表中都应该存在 gmt_create gmt_modified 并且要自动化填充
     * 方式一： 数据库默认值
     * 方式二:  程序时间
     */
    @Test
    public void updateTest() {
        AsPerson asPerson = new AsPerson();

        asPerson.setId("1486907622002008065");
        asPerson.setCusecode("02");

        int result = asPersonMapper.updateById(asPerson);

    }

    /**
     * 乐观锁实现 成功操作
     */
    public void lockTest() {
        //查询出用户信息
        AsPerson person = asPersonMapper.selectById("1486912101808205825");
        //修改内容
        person.setCusecode("wxh");
        //更新数据库
        asPersonMapper.updateById(person);
    }

    /**
     * 乐观锁实现  失败操作
     */
    @Test
    public void lockFailTest() {
        //线程1
        AsPerson thread1 = asPersonMapper.selectById("1486912101808205825");
        thread1.setCusecode("thread-person-6");
        //线程2
        AsPerson thread2 = asPersonMapper.selectById("1486912101808205825");
        thread2.setCusecode("thread-person-7");

        asPersonMapper.updateById(thread2);

        //如果没有锁 thread-person-6 会覆盖掉   thread-person-7
        //加锁后无法覆盖
        asPersonMapper.updateById(thread1);

    }

    /**
     * 查询数据，传入多个 ID 作为条件
     * 1486912101808205825
     * 1486960562897162242
     * 1486960615095324674
     */
    @Test
    public void selectByBatchIDTest() {
        List<AsPerson> asPersonList = asPersonMapper.selectBatchIds(Arrays.asList("1486912101808205825", "1486960562897162242", "1486960615095324674"));
        asPersonList.forEach(System.out::println);
    }

    /**
     * 简单的条件查询
     * 都是并且条件
     */
    @Test
    public void selectByConditionTest() {
        HashMap condition = new HashMap();
        condition.put("icorp", "1");
        List<AsPerson> asPersonList = asPersonMapper.selectByMap(condition);
        asPersonList.forEach(System.out::println);
    }

    /**
     * 逻辑删除
     */
    @Test
    public void deleteFictitiousTest(){
        asPersonMapper.deleteById("1486912101808205825");
    }

    /**
     * 传统Mybatis 查询
     * 复杂的SQl 使用这种
     */
    @Test
    public void selectByXmlTest(){
       List<HashMap> personMaps =  asPersonMapper.selectByCorp("1");
       personMaps.forEach(System.out::println);
    }

    /**
     * 执行@Select注解sql
     */
    @Test
    public void selectByDynamicTest(){
        HashMap param = new HashMap();
        param.put("imenu","as_menup2103131585300496357826568150");
        List<HashMap> personMaps = asPersonMapper.selectByDynamicSql(param);
        personMaps.forEach(System.out::println);

    }

    /**
     * 执行动态SQL
     */
    @Test
    public void selectBySqlStringTest(){
        String sql = "select * from as_person";
        List<HashMap> result = myBaseMapper.selectDynamicSql(sql);
        result.forEach(System.out::println);
    }

    /**
     * 测试kafka 表
     */
    @Test
    public void kafkaTableTest(){
        KafkaOffset kafkaOffset = new KafkaOffset();
        kafkaOffset.setTopic("test");

        kafkaOffsetMapper.insert(kafkaOffset);

        List<KafkaOffset> kafkaOffsets =  kafkaOffsetMapper.selectList(null);
        kafkaOffsets.forEach(System.out::println);



    }
}
