package com.sam.elasticsearchdemo;

import com.sam.elasticsearchdemo.pojo.Item;
import com.sam.elasticsearchdemo.repository.ItemRepository;
import lombok.val;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author Sam
 * @date 2019/2/17
 * @time 11:24
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class ElasticsearchTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    private ItemRepository itemRepository;

    @Test
    public void createIndex() {
        // 创建索引 @Document(indexName = "item", type = "docs", shards = 3, replicas = 2)
        elasticsearchTemplate.createIndex(Item.class);
    }

    @Test
    public void createMapping() {
        // 创建映射关系
        elasticsearchTemplate.putMapping(Item.class);
    }

    @Test
    public void save() {
        // 添加数据
        val item = new Item();
        item.setId(1L);
        item.setTitle("小米手机");
        item.setCategory("手机");
        item.setBrand("小米");
        item.setPrice(3899.99);
        item.setImages("www.baidu.com");
        itemRepository.save(item);
    }

    @Test
    public void batchSave() {
        // 批量添加数据
        List<Item> items = new ArrayList<>();
        val item = new Item();
        item.setId(2L);
        item.setTitle("坚果手机R1");
        item.setCategory("手机");
        item.setBrand("锤子");
        item.setPrice(3999.99);
        item.setImages("www.baidu.com");
        val item2 = new Item();
        item2.setId(3L);
        item2.setTitle("华为META10");
        item2.setCategory("手机");
        item2.setBrand("华为");
        item2.setPrice(4000.99);
        item2.setImages("www.baidu.com");
        items.add(item);
        items.add(item2);
        itemRepository.saveAll(items);
    }

    @Test
    public void update() {
        // 和新增一致， 修改数据ID相同即可
        val item = new Item();
        item.setId(2L);
        item.setTitle("坚果手机R1");
        item.setCategory("手机");
        item.setBrand("锤子");
        item.setPrice(3999.99);
        item.setImages("www.baidu.com");
        this.itemRepository.save(item);
    }

    @Test
    public void findAll() {
        // 查询所有 按照价格排序
        Iterable<Item> all = this.itemRepository.findAll(Sort.by(Sort.Direction.DESC,"price"));
        all.forEach(item -> System.out.println(item));
    }


    @Test
    public void findById() {
        Optional<Item> byId = this.itemRepository.findById(1L);
        System.out.println(byId);
    }


    @Test
    public void addCustom() {
        List<Item> list = new ArrayList<>();
        list.add(new Item(5L, "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(6L, "果手机R1", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(7L, "为META10", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(8L, "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(9L, "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));
        // 接收对象集合，实现批量新增
        itemRepository.saveAll(list);
    }

    @Test
    public void findCustom() {
        // 自定义查询
        List<Item> itemByPriceBetween = this.itemRepository.findItemByPriceBetween(3000.00, 4000.00);
        itemByPriceBetween.forEach(item -> System.out.println(item));

    }

    @Test
    public void testQuery() {
        // Repository的search方法需要QueryBuilder参数，elasticSearch为我们提供了一个对象QueryBuilders：
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", "小米");
        Iterable<Item> search = this.itemRepository.search(matchQueryBuilder);
        search.forEach(item -> System.out.println(item));
    }


    @Test
    public void NativeQuery() {
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本的分词查询
        queryBuilder.withQuery(QueryBuilders.matchQuery("title", "小米手机"));
        // 执行搜索结果
        Page<Item> search = this.itemRepository.search(queryBuilder.build());
        // 打印总条数
        System.out.println(search.getTotalElements());
        // 打印总页数
        System.out.println(search.getTotalPages());
    }


    @Test
    public void testPageNaviteQuery() {
        // 分页查询

        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本分词查询
        queryBuilder.withQuery(QueryBuilders.matchQuery("categroy", "手机"));

        //初始化分页参数
        int page = 0;
        int size = 3;
        // 设置分页参数
        queryBuilder.withPageable(PageRequest.of(page, size));

        // 执行搜索结果
        Page<Item> search = this.itemRepository.search(queryBuilder.build());
        // 打印总条数
        System.out.println(search.getTotalElements());
        // 打印总页数
        System.out.println(search.getTotalPages());
        // 每页大小
        System.out.println(search.getSize());
        // 当前页
        System.out.println(search.getNumber());
    }

    @Test
    public void testSort() {
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.termQuery("title", "小米"));

        // 排序
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));

        // 执行搜索，获取结果
        Page<Item> search = this.itemRepository.search(queryBuilder.build());
        search.forEach(item -> System.out.println(item));
    }



    //--------------------------------------聚合--------------------------------------------


    @Test
    public void testAgg() {
    // 根据brand进行分组

        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        // 不查询任何结果
        searchQueryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""}, null));

        // 1.添加一个新的聚合，聚合类型为terms,聚合名称为brands, 聚合字段为brand
        searchQueryBuilder.addAggregation(AggregationBuilders.terms("brands").field("brand"));
        // 2.查询
        AggregatedPage<Item> aggPage = (AggregatedPage<Item>) this.itemRepository.search(searchQueryBuilder.build());
        // 3.解析
        // 3.1.从结果中取出名为brands的聚合
        StringTerms stringTerms = (StringTerms) aggPage.getAggregation("brands");
        // 3.2.获取桶
        List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
        for (StringTerms.Bucket bucket : buckets) {
            // 获取品牌名称
            System.out.println(bucket.getKeyAsString());
            // 获取同种的文档数量
            System.out.println(bucket.getDocCount());

        }
    }


    @Test
    public void testSubAgg() {
        // 嵌套聚合，求平均值

        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 不查询任何结果
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""}, null));
        // 1. 添加一个新的聚合
        queryBuilder.addAggregation(AggregationBuilders.terms("brands").field("brand")
                .subAggregation(AggregationBuilders.avg("priceAvg").field("price")));

        // 2.查询，需要把结果强制转化为AggregatePage类型
        AggregatedPage<Item> aggregatedPage = (AggregatedPage<Item>) this.itemRepository.search(queryBuilder.build());

        // 3.解析
        StringTerms terms = (StringTerms) aggregatedPage.getAggregation("brands");

        // 4.获取桶
        List<StringTerms.Bucket> buckets = terms.getBuckets();
        for (StringTerms.Bucket bucket : buckets) {
            System.out.println(bucket.getKeyAsString() + "." + bucket.getDocCount());

            InternalAvg avg  = (InternalAvg) bucket.getAggregations().asMap().get("priceAvg");
            System.out.println(avg.getValue());

        }

    }

}
