package com.zh.service;

import com.alibaba.fastjson.JSON;
import com.zh.pojo.Content;
import com.zh.utils.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class ContentService {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    // 将数据存入es索引中
    public boolean parseContent(String keywords) throws IOException {

        // 解析数据
        ArrayList<Content> contents = new HtmlParseUtil().parseJD(keywords);

        // 存入es中
        BulkRequest request = new BulkRequest();
        request.timeout("10m");

        for(int i = 0;i < contents.size(); i++){
            request.add(
                    new IndexRequest("jd_goods")
                    .source(JSON.toJSONString(contents.get(i)), XContentType.JSON)
            );
        }

        BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);

        return !bulk.hasFailures();
    }

    // 获取数据实现搜索
    public List<Map<String,Object>> searchPage(String keywords,int pageNo,int pageSize) throws IOException {

        // 默认第一页
        if (pageNo < 1){
            pageNo = 1;
        }

        // 条件搜索
        SearchRequest request = new SearchRequest("jd_goods");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // 分页
        sourceBuilder.from(pageNo);
        sourceBuilder.size(pageSize);

        // 精准匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keywords);
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60,TimeUnit.SECONDS));

        // 高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title"); // 设置高亮字段
        highlightBuilder.requireFieldMatch(false); // 多个高亮显示
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        sourceBuilder.highlighter(highlightBuilder);

        // 执行搜索
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        ArrayList<Map<String, Object>> list = new ArrayList<>();

        // 解析结果
        for (SearchHit hit : response.getHits().getHits()) {

            // 获取高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            // 获取所有title
            HighlightField title = highlightFields.get("title");
            // 原来的结果
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            // 解析高亮字段，将原来的字段替换为高亮字段即可
            if (title != null){
                // 取出高亮字段
                Text[] fragments = title.fragments();
                String n_title = "";
                for (Text text : fragments) {
                    n_title += text;
                }
                sourceAsMap.put("title",n_title);  // 高亮字段替换掉原来的内容
            }

            list.add(sourceAsMap);

        }

        return list;

    }
}
