package com.sam.elasticsearchdemo.repository;

import com.sam.elasticsearchdemo.pojo.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * @author Sam
 * @date 2019/2/17
 * @time 16:34
 */
public interface ItemRepository extends ElasticsearchRepository<Item,Long> {

    /**
     * 根据金额查找item
     * @param price1
     * @param price2
     * @return
     */
    List<Item> findItemByPriceBetween(double price1, double price2);

}
