package com.zh;

import com.alibaba.fastjson.JSON;
import com.zh.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * ES API 测试
 */
@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    // 创建索引请求
    @Test
    void testCreateIndex() throws IOException {
        // 1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("beloved");
        // 2.客户端执行请求    请求后获得相应
        CreateIndexResponse response =
                client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(response);
    }

    // 测试获取索引,判断是否存在返回 true false
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("beloved2");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }


    // 删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("beloved");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        // 查看删除是否成功
        System.out.println(delete.isAcknowledged());
    }


    // 测试添加文档
    @Test
    void testAddDocument() throws IOException {
        // 创建对象
        User user = new User("张三", 20);

        // 创建请求                               索引名
        IndexRequest request = new IndexRequest("beloved");

        // 规则 put /beloved/_doc/1
        request.id("1");

        // 设置过期时间规则和过期时间为1s
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        // 将数据放入请求
        request.source(JSON.toJSONString(user), XContentType.JSON);

        // 客户端发送请求,获取相应结果
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(response.toString());  // 返回具体信息
        System.out.println(response.status());    // 返回状态 CREATED/UPDATE
    }

    // 获取文档 判断是否存在
    @Test
    void testIsExists() throws IOException {
        GetRequest request = new GetRequest("beloved", "1");

        // 不获取返回的_source上下文
        request.fetchSourceContext(new FetchSourceContext(false));

        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 获取文档详细信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("beloved", "1");

        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        // 获取完整信息
        System.out.println(response);
        // 获取文档内容
        System.out.println(response.getSourceAsString());
    }

    // 更新文档信息
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("beloved", "1");
        request.timeout("1s");

        User user = new User("张恒", 25);

        request.doc(JSON.toJSONString(user),XContentType.JSON);

        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        System.out.println(response);
        System.out.println(response.status());
    }


    // 删除文档
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("beloved", "1");
        request.timeout("1s");

        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);

        System.out.println(response);
        System.out.println(response.status());
    }


    // 批量操作
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> userList = new ArrayList<>();
        for (int i = 1;i <= 10; i++){
            userList.add(new User("张恒"+i, i));
        }

        // 批处理请求  BulkRequest
        for(int i = 0; i < userList.size(); i++){
            // 批量更新和删除，这在里修改即可
            bulkRequest.add(
                    new IndexRequest("beloved")
                    .id(""+(i+1))      // id可以省略，会生成一个随机id
                    .source(JSON.toJSONString(userList.get(i)),XContentType.JSON)
            );
        }

        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        // 是否失败 false成功  true失败
        System.out.println(response.hasFailures());
    }


    // 查询
    @Test
    void testSearch() throws IOException {
        SearchRequest request = new SearchRequest("beloved");

        // 构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // 查询条件  使用QueryBuilders工具类实现
        // QueryBuilders.termQuery()    精确
        // QueryBuilders.matchAllQuery()    匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "张恒");
        // MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        sourceBuilder.query(termQueryBuilder);
        // 分页
        // sourceBuilder.from();
        // sourceBuilder.size();
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response);
        System.out.println(JSON.toJSONString(response.getHits()));

        System.out.println("==============================");
        for (SearchHit documentFields : response.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }

    }


}
