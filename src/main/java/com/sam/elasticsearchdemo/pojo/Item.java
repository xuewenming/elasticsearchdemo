package com.sam.elasticsearchdemo.pojo;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * @author Sam
 * @date 2019/2/17
 * @time 11:22
 */
@Data
@Document(indexName = "item", type = "docs", shards = 3, replicas = 2)
public class Item {

    public Item(Long id, String title, String category, String brand, Double price, String images) {
        this.id = id;
        this.title = title;
        this.category = category;
        this.brand = brand;
        this.price = price;
        this.images = images;
    }

    public Item() {
    }

    @Id
    Long id;
    /**
     * 标题
     */
    @Field(type = FieldType.Text,analyzer = "ik_max_word")
    String title;
    /**
     * 分类
     */
    @Field(type = FieldType.Keyword)
    String category;
    /**
     * 品牌
     */
    @Field(type = FieldType.Keyword)
    String brand;
    /**
     * 价格
     */
    @Field(type = FieldType.Double)
    Double price;
    /**
     * 图片地址
     */
    @Field(type = FieldType.Keyword, index = false)
    String images;
}
