package com.bjst;

import com.bjst.pojo.People;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootTest
class EsdemoApplicationTests {
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;


    @Test    //此方法创建表以及表中的字段
  void contextLoads() {
        boolean result1 = elasticsearchTemplate.createIndex(People.class);  //创建索引 创建出来的索引不具备maping（字段）信息
        System.out.println(result1);
        boolean results = elasticsearchTemplate.putMapping(People.class);   //putMapping 为已有索引添加新字段 不具备索引创建能力
        System.out.println(results);
    }

    @Test    //删除索引
    void delete() {
        boolean result = elasticsearchTemplate.deleteIndex(People.class);
        System.out.println(result);
    }


    @Test
    void insert1() {  //新增一条数据      如果id存在则为修改
        People peo = new People();
        peo.setId("1");
        peo.setName("张三");
        peo.setAddress("北京市海淀区回龙观东大街");
        peo.setAge(18);
        IndexQuery query = new IndexQuery();
        query.setObject(peo);
        String result = elasticsearchTemplate.index(query);
        System.out.println(result);
    }

    @Test
    void insertBulk() {   //批量新增
        List<IndexQuery> list = new ArrayList<>();
        list.add(new IndexQueryBuilder().withObject(new People("1", "张三", "北京市海淀区回龙观东大街", 21)).build());
        list.add(new IndexQueryBuilder().withObject(new People("2", "李四", "北京市大兴区科创十四街", 22)).build());
        list.add(new IndexQueryBuilder().withObject(new People("3", "王五", "天津市北京大街", 23)).build());
        list.add(new IndexQueryBuilder().withObject(new People("4", "赵六", "上海市虹桥机场", 24)).build());
        elasticsearchTemplate.bulkIndex(list);
    }


    @Test
    void deleteDoc() {
        String result = elasticsearchTemplate.delete(People.class, "4");
        System.out.println(result);
    }


    @Test
    void query() {
        // NativeSearchQuery构造方法参数。
        // 北京去和所有field进行匹配，只要出现了北京就可以进行查询
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery("北京");
        // 查询条件SearchQuery是接口，只能实例化实现类。
        SearchQuery searchQuery = new NativeSearchQuery(queryStringQueryBuilder);
        List<People> list = elasticsearchTemplate.queryForList(searchQuery, People.class);
        for (People people : list) {
            System.out.println(people);
        }
    }

    @Test
    void matchAll() {
        SearchQuery searchQuery = new NativeSearchQuery(QueryBuilders.matchAllQuery());
        List<People> list = elasticsearchTemplate.queryForList(searchQuery, People.class);
        for (People people : list) {
            System.out.println(people);
        }
    }

    @Test
    void match() {
        SearchQuery searchQuery = new NativeSearchQuery(QueryBuilders.matchQuery("address", "我要去北京"));
        List<People> list = elasticsearchTemplate.queryForList(searchQuery, People.class);
        for (People people : list) {
            System.out.println(people);
        }
    }

    @Test
    void mathPhrase() {
        SearchQuery searchQuery = new NativeSearchQuery(QueryBuilders.matchPhraseQuery("address", "北京市"));
        List<People> list = elasticsearchTemplate.queryForList(searchQuery, People.class);
        for (People people : list) {
            System.out.println(people);
        }
    }

    @Test
    void range() {
        SearchQuery searchQuery = new NativeSearchQuery(QueryBuilders.rangeQuery("age").gte(22).lte(23));
        List<People> list = elasticsearchTemplate.queryForList(searchQuery, People.class);
        for (People people : list) {
            System.out.println(people);
        }
    }


    @Test
    void MustShould() {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        List<QueryBuilder> listQuery = new ArrayList<>();
        listQuery.add(QueryBuilders.matchPhraseQuery("name", "张三"));
        listQuery.add(QueryBuilders.rangeQuery("age").gte(22).lte(23));
//        boolQueryBuilder.should().addAll(listQuery); // 逻辑或
        boolQueryBuilder.must().addAll(listQuery);  // 逻辑与
        SearchQuery searchQuery = new NativeSearchQuery(boolQueryBuilder);
        List<People> list = elasticsearchTemplate.queryForList(searchQuery, People.class);
        for (People people : list) {
            System.out.println(people);
        }
    }


    @Test
    void PageSort() {
        SearchQuery searchQuery = new NativeSearchQuery(QueryBuilders.matchAllQuery());
        // 分页 第一个参数是页码，从0算起。第二个参数是每页显示的条数
        searchQuery.setPageable(PageRequest.of(0, 2));
        // 排序 第一个参数排序规则 DESC ASC 第二个参数是排序属性
        searchQuery.addSort(Sort.by(Sort.Direction.DESC, "age"));
        List<People> list = elasticsearchTemplate.queryForList(searchQuery, People.class);
        for (People people : list) {
            System.out.println(people);
        }
    }

    @Test
    void hl() {
        // 高亮属性
        HighlightBuilder.Field hf = new HighlightBuilder.Field("address");
        // 高亮前缀
        hf.preTags("<span>");
        // 高亮后缀
        hf.postTags("</span>");

        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder()
                // 排序
                .withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC))
                // 分页
                .withPageable(PageRequest.of(0, 2))
                // 应用高亮
                .withHighlightFields(hf)
                // 查询条件， 必须要有条件，否则高亮
                .withQuery(QueryBuilders.matchQuery("address", "北京市")).build();

        AggregatedPage<People> peoples = elasticsearchTemplate.queryForPage(searchQuery, People.class, new SearchResultMapper() {
            @Override
            public <T> AggregatedPage<T> mapResults(SearchResponse searchResponse, Class<T> aClass, Pageable pageable) {
                /*
                {
  "took" : 190,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : 3,
    "max_score" : 1.7918377,
    "hits" : [
      {
        "_index" : "people_index",
        "_type" : "people_type",
        "_id" : "1",
        "_score" : 1.7918377,
        "_source" : {
          "id" : "1",
          "name" : "张三",
          "address" : "北京市回龙观东大街",
          "age" : 21
        },
        "highlight" : {
          "address" : [
            "<span>北京</span><span>市</span>回龙观东大街"
          ]
        }
      },
      {
        "_index" : "people_index",
        "_type" : "people_type",
        "_id" : "2",
        "_score" : 1.463562,
        "_source" : {
          "id" : "2",
          "name" : "李四",
          "address" : "北京市大兴区科创十四街",
          "age" : 22
        },
        "highlight" : {
          "address" : [
            "<span>北京</span><span>市</span>大兴区科创十四街"
          ]
        }
      },
      {
        "_index" : "people_index",
        "_type" : "people_type",
        "_id" : "3",
        "_score" : 0.38845786,
        "_source" : {
          "id" : "3",
          "name" : "王五",
          "address" : "天津市北京大街",
          "age" : 23
        },
        "highlight" : {
          "address" : [
            "天津市<span>北京</span>大街"
          ]
        }
      }
    ]
  }
}

                 */

                // 取最里面层的hits
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                // 最终返回的数据
                List<T> peopleList = new ArrayList<>();
                for (SearchHit searchHit : searchHits) {
                    //需要把SearchHit转换为People
                    People peo = new People();
                    // 取出非高亮数据
                    Map<String, Object> mapSource = searchHit.getSourceAsMap();
                    // 取出高亮数据
                    Map<String, HighlightField> mapHL = searchHit.getHighlightFields();
                    // 把非高亮数据进行填充
                    peo.setId(mapSource.get("id").toString());
                    peo.setName(mapSource.get("name").toString());
                    peo.setAge(Integer.parseInt(mapSource.get("id").toString()));
                    // 判断是否有高亮，如果只有一个搜索条件，一定有这个高亮数据，if可以省略
                    if (mapHL.containsKey("address")) {
                        // 设置高亮数据
                        peo.setAddress(mapHL.get("address").getFragments()[0].toString());
                    }
                    // 把people添加到集合中
                    peopleList.add((T) peo);
                }
                // 如果没有分页，只有第一个参数。
                // 总条数在第一个hits里面。
                return new AggregatedPageImpl<>(peopleList, pageable, searchResponse.getHits().totalHits);
            }

            @Override
            public <T> T mapSearchHit(SearchHit searchHit, Class<T> aClass) {
                return null;
            }
        });

        // 通过getContent查询出最终List<People>
        for (People people : peoples.getContent()) {
            System.out.println(people);
        }
    }


}
