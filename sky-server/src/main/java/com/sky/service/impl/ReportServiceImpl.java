package com.sky.service.impl;

import com.sky.dto.GoodsSalesDTO;
import com.sky.entity.Orders;
import com.sky.mapper.OrderMapper;
import com.sky.mapper.UserMapper;
import com.sky.service.ReportService;
import com.sky.service.WorkspaceService;
import com.sky.vo.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ReportServiceImpl implements ReportService {
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private UserMapper userMapper;
    @Autowired
    private WorkspaceService workspaceService;


    /**
     * 统计指定时间区间内的营业额数据：查询的是订单表，状态已经完成的订单
     * @param begin
     * @param end
     * @return
     */
    @Override
    public TurnoverReportVO getTurnover(LocalDate begin, LocalDate end) {
        //1.日期：当前集合用于存放从begin到end范围内的每天的日期
        List<LocalDate> dateList = new ArrayList<>();
        dateList.add(begin);

        while (!begin.equals(end)){
            //日期计算，获得指定日期后1天的日期
            begin = begin.plusDays(1);
            dateList.add(begin);
        }

        //现在是list类型而vo类中的dateList要求是String类型，所以需要进行转化
        //把list集合的每个元素取出来并且以逗号分隔，最终拼成一个字符串
        String data = StringUtils.join(dateList, ",");


        //2.营业额：是和日期一一对应的，所以需要遍历获取每天的日期，然后查询数据库把每天的营业额查出来
        //         中间在以逗号隔开。
        //当前集合用于存放存放每天的营业额
        List<Double> turnoverList = new ArrayList<>();
        for (LocalDate date : dateList) {
            //查询data日期对应的营业额数据，营业额是指：状态为“已完成”的订单金额合计
            //获取的date（每日）只含有年月日没有体现时分秒，而这个order_time下单时间是
            //      LocalDateTime类型，既有年月日又有时分秒，所以要查询date这一天的
            //      订单就需要来计算这一天的起始时间是从什么时刻开始（当天的0时0分0秒），
            //      这一天的结束时间是从什么时候结束（11:59:59，无限接近下一天的0时0分0秒）。
            LocalDateTime beginTime = LocalDateTime.of(date, LocalTime.MIN);//获取当天的开始时间：年月日时分秒
            LocalDateTime endTime = LocalDateTime.of(date, LocalTime.MAX);//获取当天的结束时间：年月日时分秒（无限接近于下一天的0时0分0秒）

            //select sum(amount) from orders where order_time > ? and order_time < ? and status = 5;
            Map map = new HashMap();//封装sql所需要的参数为一个map集合
            map.put("begin",beginTime);
            map.put("end", endTime);
            map.put("status", Orders.COMPLETED);//已完成
            Double turnover = orderMapper.sumByMap(map);//计算出来的营业额
            //假设这一天一个订单都没有，那么营业额应该是0.0才对，而这里查询出的来营业额
            //   为空显然不合理，所以如果返回为空的时候需要把它转化为0.0。
            turnover = turnover == null ? 0.0 : turnover;
            turnoverList.add(turnover);
        }

        //同样需要把集合类型转化为字符串类型并用逗号分隔
        String turnover = StringUtils.join(turnoverList, ",");


        //构建VO对象
        TurnoverReportVO trvo = TurnoverReportVO
                .builder()
                .dateList(data)
                .turnoverList(turnover)
                .build();

        return trvo;
    }

    @Override
    public UserReportVO getUserStatistics(LocalDate begin, LocalDate end) {
        //1.准备日期条件：和营业额功能相同，不在赘述。
        List<LocalDate> dateList = new ArrayList<>();
        dateList.add(begin);

        while (!begin.equals(end)){
            begin = begin.plusDays(1);
            dateList.add(begin);
        }
        //同样需要把集合类型转化为字符串类型并用逗号分隔
        String data = StringUtils.join(dateList, ",");

        //2.准备每一天对应的用户数量：总用户数量和新增用户数量    查询的是用户表
        List<Integer> newUserList = new ArrayList<>(); //此集合保存新增用户数量
        List<Integer> totalUserList = new ArrayList<>(); //此集合保存总用户数量

        /**
         * 思路分析：
         * 当天新增用户数量：只需要根据注册时间计算出当天的起始时间和结束时间作为查询条件，
         *                就是当天新增的用户数量。
         * 当天总用户数量：比如统计截止到4月1号这一天总的用户数量，意味着注册时间只要是在4月1号
         *              之前（包含4月1号）这一天的数量就可以了。
         * 没必要写2个sql，只需要写一个动态sql（动态的去拼接这2个连接条件）兼容这2个sql就可以了。
         */
        for (LocalDate date : dateList) {
            LocalDateTime beginTime = LocalDateTime.of(date, LocalTime.MIN);//起始时间 包含年月日时分秒
            LocalDateTime endTime = LocalDateTime.of(date, LocalTime.MAX);//结束时间

            //新增用户数量 select count(id) from user where create_time > ? and create_time < ?
            Integer newUser = getUserCount(beginTime, endTime);
            //总用户数量 select count(id) from user where  create_time < ?
            Integer totalUser = getUserCount(null, endTime);

            newUserList.add(newUser);//把查询到的数据添加到集合中保存
            totalUserList.add(totalUser);
        }

        //同样需要把集合类型转化为字符串类型并用逗号分隔
        String newUser = StringUtils.join(newUserList, ",");
        String totalUser = StringUtils.join(totalUserList, ",");

        //封装vo返回结果
        return UserReportVO.builder()
                .dateList(data)
                .newUserList(newUser)
                .totalUserList(totalUser)
                .build();
    }
    /**
     * 根据时间区间统计用户数量
     * @param beginTime
     * @param endTime
     * @return
     */
    private Integer getUserCount(LocalDateTime beginTime, LocalDateTime endTime) {
        //封装sql查询的条件为map集合，因为设计的mapper层传递的参数是使用map来封装的
        Map map = new HashMap();
        map.put("begin",beginTime);
        map.put("end", endTime);
//        return userMapper.countByMap(map);
        return userMapper.countByMap(map);
    }

    /**
     * 根据时间区间统计订单数量
     * @param begin
     * @param end
     * @return
     */
    @Override
    public OrderReportVO getOrderStatistics(LocalDate begin, LocalDate end){
        //1.准备日期条件：和营业额功能相同，不在赘述。
        List<LocalDate> dateList = new ArrayList<>();
        dateList.add(begin);

        while (!begin.equals(end)){
            begin = begin.plusDays(1);
            dateList.add(begin);
        }
        //同样需要把集合类型转化为字符串类型并用逗号分隔
        String data = StringUtils.join(dateList, ",");

        //2.准备每一天对应的订单数量：订单总数  有效订单数
        //每天订单总数集合
        List<Integer> orderCountList = new ArrayList<>();
        //每天有效订单数集合
        List<Integer> validOrderCountList = new ArrayList<>();

        /**
         * 思路分析：查询的是订单表
         * 查询每天的总订单数：只需要根据下单时间计算出当天的起始时间和结束时间作为查询条件，
         *                 就是当天的总订单数。
         * 查询每天的有效订单数：根据下单时间计算出当天的起始时间和结束时间以及状态已完成（代表有效订单）的订单作为查询条件，
         *                   就是每天的有效订单数
         * 同样没必要写2个sql，因为这2个SQL的主体结构相同，只是查询的条件不同，所以没有必要写2个sql只需要写一个动态的sql即可。
         */
        for (LocalDate date : dateList) {
            LocalDateTime beginTime = LocalDateTime.of(date, LocalTime.MIN);//起始时间 包含年月日时分秒
            LocalDateTime endTime = LocalDateTime.of(date, LocalTime.MAX);//结束时间
            //查询每天的总订单数 select count(id) from orders where order_time > ? and order_time < ?
            Integer orderCount = getOrderCount(beginTime, endTime, null);

            //查询每天的有效订单数 select count(id) from orders where order_time > ? and order_time < ? and status = ?
            Integer validOrderCount = getOrderCount(beginTime, endTime, Orders.COMPLETED);

            orderCountList.add(orderCount);
            validOrderCountList.add(validOrderCount);
        }

        //同样需要把集合类型转化为字符串类型并用逗号分隔
        String orderCount1 = StringUtils.join(orderCountList, ",");//每天订单总数集合
        String validOrderCount1 = StringUtils.join(validOrderCountList, ",");//每天有效订单数集合

        /**
         * 3. 准备时间区间内的订单数：时间区间内的总订单数   时间区间内的总有效订单数
         * 思路分析：
         *    订单总数：整个这个时间区域内订单总的数量，根据这个时间去卡查询数据库可以查询出来。
         *    时间区间内的总有效订单数：也可以通过查询数据库查出来。
         *    实际上不查询数据库，总订单数和总有效订单数也能计算出来：
         *         因为上面2个集合中已经查询保存了，这个时间段之内每天的总订单数和总有效订单数，
         *         所以只需要分别遍历这2个集合，每天的订单总数加一起就是整个时间段总订单数，
         *         每天的有效订单数加起来就是整个时间段的总有效订单数。
         */
        //计算时间区域内的总订单数量
        //Integer totalOrderCounts = orderCountList.stream().reduce(Integer::sum).get();//方式一：简写方式
        Integer totalOrderCounts = 0;
        for (Integer integer : orderCountList) {  //方式二：普通for循环方式
            totalOrderCounts = totalOrderCounts+integer;
        }
        //计算时间区域内的总有效订单数量
        //Integer validOrderCounts = validOrderCountList.stream().reduce(Integer::sum).get();//方式一：简写方式
        Integer validOrderCounts = 0;
        for (Integer integer : validOrderCountList) { //方式二：普通for循环方式
            validOrderCounts = validOrderCounts+integer;
        }

        //4.订单完成率：  总有效订单数量/总订单数量=订单完成率
        Double orderCompletionRate = 0.0;  //订单完成率的初始值
        if(totalOrderCounts != 0){ //防止分母为0出现异常
            //总有效订单数量和总有效订单数量都是Integer类型，这里使用的是Double类型接收所以需要进行转化
            orderCompletionRate = validOrderCounts.doubleValue() / totalOrderCounts;
        }

        //构造vo对象
        return OrderReportVO.builder()
                .dateList(data)  //x轴日期数据
                .orderCountList(orderCount1) //y轴每天订单总数
                .validOrderCountList(validOrderCount1)//y轴每天有效订单总数
                .totalOrderCount(totalOrderCounts) //时间区域内总订单数
                .validOrderCount(validOrderCounts) //时间区域内总有效订单数
                .orderCompletionRate(orderCompletionRate) //订单完成率
                .build();

    }
    /**
     * 根据时间区间统计指定状态的订单数量
     * @param beginTime
     * @param endTime
     * @param status
     * @return
     */
    private Integer getOrderCount(LocalDateTime beginTime, LocalDateTime endTime, Integer status) {
        //封装sql查询的条件为map集合，因为设计的mapper层传递的参数是使用map来封装的
        Map map = new HashMap();
        map.put("status", status);
        map.put("begin",beginTime);
        map.put("end", endTime);
        return orderMapper.countByMap(map);
    }

    /**
     * 4)查询指定时间区间内的销量排名top10
     * 思路分析：
     *   查询订单详情表的number字段，该字段体现商品的销售份数。
     *   用户下单就会产生订单数据和订单详情数据，如果用户下完单又取消掉我们并没有
     *      真正的把这个数据给删除，而是把订单的状态给修改了，此时在统计数据这个
     *      取消状态的订单就不能在算数了，我们要统计的实际上是完成状态的订单。
     *   当前订单详情表不能体现出订单的状态，具体的订单状态还要查询订单表才可以看出来，
     *      所以需要多表联合查询。
     *
     * sql：SELECT od.`name`,SUM(od.`number`) number FROM order_detail od ,orders o
     *       WHERE od.`order_id`=o.`id`AND STATUS = 5 AND o.`order_time`>'2022-10-1' AND o.`order_time`<'2024-10-31'
     *       GROUP BY od.`name`
     *       ORDER BY number DESC
     *       LIMIT 0,10;
     * sql解释：
     *  多表查询商品的名称（name字段）以及销量（number字段，数据有可能有多条所以需要求和）
     *    查询条件：
     *       订单状态：为5已完成
     *       下单时间：销量排名统计的实际上是某个时间区间内的销量排名，所以还需要根据下单时间去卡。
     *    分组：统计销量的时候要根据商品去统计，也就是说相同的商品这些number要累加在一起，所以说要进行分组
     *    排序：按照降序来排
     *    销量前10：统计前10条数据，即分页现显示前10条数据。
     *
     *  sql查询的结果封装到GoodsSalesDTO实体类中：商品名称  销量。
     *
     * */
    @Override
    public SalesTop10ReportVO getSalesTop10(LocalDate begin, LocalDate end){
        //mapper传递的参数是LocalDateTime类型，impl层传递的参数是LocalDate所以需要进行转化
        //   当前日期的起始时间      结束日期最后的时间点
        LocalDateTime beginTime = LocalDateTime.of(begin, LocalTime.MIN);
        LocalDateTime endTime = LocalDateTime.of(end, LocalTime.MAX);
        List<GoodsSalesDTO> goodsSalesDTOList = orderMapper.getSalesTop10(beginTime, endTime);

        /**
         * 获取的是GoodsSalesDTO类型的集合数据：String name商品名称     Integer number销量
         * 需要获得是vo对象类型： String nameList 商品名称列表   以逗号分隔，例如：鱼香肉丝,宫保鸡丁,水煮鱼
         *                    String numberList销量列表     以逗号分隔，例如：260,215,200
         * 所以需要进行转化：取出GoodsSalesDTO集合中所有的name属性取出来拼接到一起，并且以逗号分隔
         *                                             （恰好对应vo中的nameList）
         *               取出GoodsSalesDTO集合中所有的number属性取出来拼接到一起，并且以逗号分隔
         *                                             （恰好对应vo中的numberList）
         *   方式一：通过 stream流的方式简写
         *   String nameList = StringUtils.join(goodsSalesDTOList.stream().map(GoodsSalesDTO::getName).collect(Collectors.toList()),",");
         *   String numberList = StringUtils.join(goodsSalesDTOList.stream().map(GoodsSalesDTO::getNumber).collect(Collectors.toList()),",");
         */
        List<String> nameList1 = new ArrayList<>(); //商品名称
        List<Integer> numberList1 = new ArrayList<>(); //销量
        for (GoodsSalesDTO goodsSalesDTO : goodsSalesDTOList) {//方式二：普通for循环
            nameList1.add(goodsSalesDTO.getName());
            numberList1.add(goodsSalesDTO.getNumber());
        }

        //获取的是list集合类型，需要转化为字符串并以逗号分隔
        //把list集合的每个元素取出来并且以逗号分隔，最终拼成一个字符串
        String nameList = StringUtils.join(nameList1, ",");
        String numberList = StringUtils.join(numberList1, ",");


        //封装vo对象并返回
        return SalesTop10ReportVO.builder()
                .nameList(nameList)
                .numberList(numberList)
                .build();
    }

    /**
     * 导出近30天的运营数据报表
     * @param response
     **/
    @Override
    public void exportBusinessData(HttpServletResponse response) {
        //1.查询数据库，获取营业数据---查询最近30天的营业数据
        /* 最近30天：
         *     LocalDate.now()：获取当天时间日期
         *     LocalDate.now().minusDays(30)：获取相对今天来说往前倒30天的日期
         *     LocalDate.now().minusDays(1)：查到相对今天的前一天（昨天），为什么
         *          不查到今天呢？？？   因为今天有可能还没有结束，数据可能还会发生变动。
         * */
        LocalDate begin = LocalDate.now().minusDays(30);//从哪天开始查（30天之前）
        LocalDate end = LocalDate.now().minusDays(1);//查到那一天（昨天）

        //构建的开始结束时间是LocalDate（年月日类型），业务层需要的是LocalDateTime（年月日时分秒）类型，所以需要进行转化。
        LocalDateTime begins = LocalDateTime.of(begin, LocalTime.MIN);//起始日期的最开始时间：0时0分0秒
        LocalDateTime ends = LocalDateTime.of(end, LocalTime.MAX);//截止日期的最晚结束时间：11:59:59

        //查询“概览数据”：在工作台页面功能中已经查过概览数据了，所以需要之前查询概览数据的业务层对象WorkspaceService

        BusinessDataVO businessData = workspaceService.getBusinessData(begins, ends);

        //2.通过poi将数据写入到Excel文件中
        /*
         * this.getClass()：获得本类的类对象
         * this.getClass().getClassLoader()：获得类加载器
         * this.getClass().getClassLoader().getResourceAsStream()：获取类路径下的指定文件的输入流
         * */
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("template/运营数据报表模板.xlsx");

        XSSFWorkbook excel = null;
        ServletOutputStream out = null;

        try {
            //基于模版文件创建一个新的excel文件对象：需要传递一个输入流对象来读取项目中的Excel模版文件
            excel = new XSSFWorkbook(inputStream);
            //获得Excel文件中的一个Sheet页
            XSSFSheet sheet = excel.getSheet("Sheet1");//根据标签页的名字

            //填充：时间区间数据
            //获取第二行的第二个单元格（通过查看excel表格可知）,并往单元格内设置时间数据
            // 注意：此时的行和单元格已存在了所以不需要创建用的是get方法获取。
            //      下标是从0开始
            sheet.getRow(1).getCell(1).setCellValue("时间："+begin+"至"+end);

            //填充：概览数据
            //获得第4行   位置查看excel表格可知
            XSSFRow row = sheet.getRow(3);
            row.getCell(2).setCellValue(businessData.getTurnover());//获取第4行的第3个单元格，并往单元格内设置数据（营业额）
            row.getCell(4).setCellValue(businessData.getOrderCompletionRate());//第5个单元格，设置（订单完成率）
            row.getCell(6).setCellValue(businessData.getNewUsers());//第7个单元格，设置（新增用户数）
            //获得第5行   位置查看excel表格可知
            row = sheet.getRow(4);
            row.getCell(2).setCellValue(businessData.getValidOrderCount());//第5行第3个单元格，设置（有效订单）
            row.getCell(4).setCellValue(businessData.getUnitPrice());//第5行第5个单元格，设置（平均客单价）

            //填充：明细数据
            /*思路分析：查看excel表格可知，概览数据和明细数据的各个数据项都是一样的，区别
                      概览数据查询的是近30天的，而这个明细数据查询的是每一天的数据项。上面
                      已经计算出了开始日期和结束日期，那么就可以往后推循环遍历30次，这个
                      次数是可以确定的。
            */
            for(int i=0;i<30;i++){
                //第一次循环i=0所以是自身的这一天，第二次循环i=1所以往后加一天，一直遍历30次，这样就可以把最近这30天都给遍历出来了
                LocalDate localDate = begin.plusDays(i);//起始时间每次往后加一天，一直遍历30次
                //获取当天的起始时间并转化为年月日时分秒类型
                LocalDateTime beginl = LocalDateTime.of(localDate, LocalTime.MIN);//每天的最开始时间：0时0分0秒
                LocalDateTime endl = LocalDateTime.of(localDate, LocalTime.MAX);//每天的最晚结束时间：11:59:59
                //查询某一天的营业数据
                BusinessDataVO businessData1 = workspaceService.getBusinessData(beginl, endl);

                //获得某一行：查看excel表格可知数据是从第8行（下标为7）开始填充的，由于i初始值为0所以从7开始每次加i就是从第8行开始往下填充数据
                row = sheet.getRow(7 + i);
                row.getCell(1).setCellValue(localDate.toString());//当前行的第2个单元格，填充日期，只能接收字符串类型所以需要转化
                row.getCell(2).setCellValue(businessData1.getTurnover());//当前行的第3个单元格，填充营业额
                row.getCell(3).setCellValue(businessData1.getValidOrderCount());//当前行的第4个单元格，填充有效订单
                row.getCell(4).setCellValue(businessData1.getOrderCompletionRate());//当前行的第5个单元格，填充订单完成率
                row.getCell(5).setCellValue(businessData1.getUnitPrice());//当前行的第6个单元格，填充平均客单价
                row.getCell(6).setCellValue(businessData1.getNewUsers());//当前行的第7个单元格，填充新增用户数
            }

            //3.通过输出流将Excel文件下载到客户端浏览器
            out = response.getOutputStream();
            excel.write(out);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            //关闭资源
            try {
                out.close();
                excel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }


}

