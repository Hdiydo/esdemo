package com.bjst.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;


@NoArgsConstructor
@AllArgsConstructor
@ToString
@Data
@Document(indexName = "people_index",type = "people_type",shards = 1,replicas = 1)
public class People {
    @Id
    private String id;
    // 整个name不被分词，切不创建索引
    // Keyword表示不被分词
    @Field(type= FieldType.Keyword,index = true)
    private String name;
    // address被ik分词
    // Text类型的属性才能被分词
    @Field(type = FieldType.Text,analyzer = "ik_max_word")
    private String address;

    @Field(type = FieldType.Long,index = true)
    private int age;

}
