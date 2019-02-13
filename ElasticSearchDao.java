package com.pcitc.imp.bizlog.dal.dao.base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Repository;

import com.pcitc.imp.bizlog.dal.dao.impl.Param;
import com.pcitc.imp.bizlog.dal.pojo.App;
import com.pcitc.imp.bizlog.dal.pojo.Log;
import com.pcitc.imp.bizlog.exception.BusiException;
import com.pcitc.imp.bizlog.service.model.Condition;
import com.pcitc.imp.bizlog.util.CheckPrompt;
import com.pcitc.imp.bizlog.util.CheckUtil;
import com.pcitc.imp.bizlog.util.ClientFactory;
import com.pcitc.imp.bizlog.util.ErrorCodeEnum;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * 用ES实现的持久层
 *
 * @author haiwen.wang
 */
@Repository
public class ElasticSearchDao {

	public ClientFactory getClientFactory() {
		return ClientFactory.getInstance();
	}

	/**
	 * 创建appregister索引
	 *
	 * @throws BusiException
	 */
	public void creatIndex(String url, String body) throws BusiException {
		RestClient client = null;
		try {
			client = getClientFactory().getClient();
			Map<String, String> params = new HashMap<String, String>();
			HttpEntity entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
			client.performRequest("PUT", url, params, entity);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, e.getMessage(), e);
		}
	}

	/**
	 * @param indexName
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<String> queryAll(String indexName) throws IOException {
		RestClient client = getClientFactory().getClient();
		String url = "/" + indexName + "/_search";
		Map params = new HashMap<>();
		params.put("size", String.valueOf(getClientFactory().getConfig().getInteger("buzilog.size")));  
		List<String> result = performGet(client, url, params, null);
		return result;
	}

	/**
	 * 从GET方法的响应中取到数据
	 * @param client
	 * @param url
	 * @param params
	 * @param entity
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<String> performGet(RestClient client, String url, Map params, HttpEntity entity) throws IOException {
		List<String> result = new ArrayList<String>();
		Response response;
		if (entity == null) {
			response = client.performRequest("GET", url, params);
		} else {
			response = client.performRequest("GET", url, params, entity);
		}
		String retStr = EntityUtils.toString(response.getEntity());
		JsonObject retJson = new JsonObject(retStr);
		Integer count = retJson.getJsonObject("hits").getInteger("total");
		JsonArray hits = retJson.getJsonObject("hits").getJsonArray("hits");
		if (hits == null || hits.isEmpty()) {
			return result;
		}
		result.add(count.toString());
		for (int i = 0; i < hits.size(); i++) {
			result.add(hits.getJsonObject(i).getJsonObject("_source").toString());
		}
		return result;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<String> performGetUpdate(RestClient client, String url, Map params, HttpEntity entity) throws IOException {
		List<String> result = new ArrayList<String>();
		Response response;
		if (entity == null) {
			response = client.performRequest("GET", url, params);
		} else {
			response = client.performRequest("GET", url, params, entity);
		}
		String retStr = EntityUtils.toString(response.getEntity());
		JsonObject retJson = new JsonObject(retStr);
		JsonArray hits = retJson.getJsonObject("hits").getJsonArray("hits");
		if (hits == null || hits.isEmpty()) {
			return result;
		}
		for (int i = 0; i < hits.size(); i++) {
			result.add(hits.getJsonObject(i).getJsonObject("_source").toString());
			result.add(hits.getJsonObject(i).getString("_id"));
		}
		return result;
	}
	
	/**
	 * 根据条件查询数据
	 * @param indexName
	 * @param paramMap
	 * @param skip
	 * @param top
	 * @return
	 */
	public List<String> queryByCon(String indexName, Map<String, Param> paramMap, String skip, String top, Condition condition)
			throws BusiException {
		RestClient client = getClientFactory().getClient();
		String url = "/" + indexName + "/_search";
		Map<String,String> params = new HashMap<String,String>();
		if (!CheckUtil.checkStringIsNull(skip)) {
			params.put("from", skip);
		}
		if (!CheckUtil.checkStringIsNull(top)) {
			params.put("size", top);
		}
		JsonArray should = new JsonArray();
		JsonArray must = new JsonArray();
		JsonArray filter = new JsonArray();
		JsonObject range = new JsonObject();
		JsonObject item_p = new JsonObject();
		Set<String> keys = paramMap.keySet();
		String keyV = null;
		for ( String key : keys) {
			keyV = key;
			Param param = paramMap.get(key);
			if(key != "timestamp"){
				if (param.getQueryType().equals("should")) {
					setParam(should, key, param);
				} else if (param.getQueryType().equals("must")) {
					setParam(must, key, param);
				} else if (param.getQueryType().equals("filter")) {
					setParam(filter, key, param);
				}
			}else{
				param.getMatchType().equals("range");
				JsonObject item = new JsonObject();
					item.put("gte", condition.getStartTime());
					item.put("lte", condition.getEndTime());
					range.put("timestamp", item);
					item_p.put(param.getMatchType(), range);
			}
		}
		JsonObject query = new JsonObject();
		if(keyV != "timestamp"){
		JsonObject con_p = new JsonObject();
		con_p.put("must", must);
		con_p.put("should", should);
		con_p.put("filter", filter);

		JsonObject bool = new JsonObject();
		bool.put("bool", con_p);
		query.put("query", bool);
		}else{
		query.put("query", item_p);
		
		}
		HttpEntity entity = new NStringEntity(query.toString(), ContentType.APPLICATION_JSON);
		List<String> result = null;
		try {
			result = performGet(client, url, params, entity);
		} catch (IOException e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.LOG_NOT_EXIST, e);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, e.getMessage(), e);
		} 
		return result;
	}

	@SuppressWarnings("rawtypes")
	private void setParam(JsonArray queryJson, String key, Param param) {
		List values = param.getValue();
		for (int i = 0; i < values.size(); i++) {
			JsonObject item = new JsonObject();
			item.put(key, values.get(i));
			JsonObject item_p = new JsonObject();
			item_p.put(param.getMatchType(), item);
			queryJson.add(item_p);
		}
	}

	/**
	 * 根据某个字段查询数据
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<String> queryByField(String indexName, String fieldName, String value) throws BusiException {
		try {
			RestClient client = getClientFactory().getClient();
			String url = "/" + indexName + "/_search";
			Map params = new HashMap<>();
			params.put("q", fieldName + ":" + value);
			List<String> result = performGet(client, url, params, null);
			return result;
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.QUERY + ":" + e.getMessage(), e);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<String> queryByFieldUpdate(String indexName, String fieldName, String value) throws BusiException {
		try {
			RestClient client = getClientFactory().getClient();
			String url = "/" + indexName + "/_search";
			Map params = new HashMap<>();
			params.put("q", fieldName + ":" + value);
			List<String> result = performGetUpdate(client, url, params, null);
			return result;
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.QUERY + ":" + e.getMessage(), e);
		}
	}

	/**
	 * @return void 返回类型
	 * @throws BusiException
	 * @Title: insert
	 * @Description: 添加
	 */
	@SuppressWarnings("unused")
	public <E> void insert(String tableName, String typeName, List<E> pojo) throws BusiException {
		try {
			RestClient client = getClientFactory().getClient();
			StringBuilder bodyStr = new StringBuilder();
			bodyStr.append("{ \"index\" : { \"_index\" : \"" + tableName + "\", \"_type\" : \"" + typeName + "\"} }"+"\r\n");
			for (int i = 0; i < pojo.size(); i++) {
				bodyStr.append(Json.encode(pojo.get(i))+"\r\n");
			}
			Map<String, String> params = new HashMap<String, String>();
			HttpEntity entity = new NStringEntity(bodyStr.toString(), ContentType.APPLICATION_JSON);
			Response indexResponse = client.performRequest("POST", "/_bulk",params,entity);
		} catch (IOException e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.INSERT + ":" + e.getMessage(), e);
		}
	}
	
	/**
	 * @Title: insertApp
	 * @Description: 添加应用
	 * @return void 返回类型
	 * @throws BusiException
	 */
	@SuppressWarnings("unused")
	public int insertApp(String tableName, String typeName, List<App> pojo) throws BusiException {
		RestClient client = getClientFactory().getClient();
		int i = 0;
		try {
			List<String> lists = new ArrayList<>();
			StringBuilder bodyStr = new StringBuilder();
			Map<String, String> params = new HashMap<String, String>();
			for (App app : pojo) {
				bodyStr.append("{ \"index\" : { \"_index\" : \"" + tableName + "\", \"_type\" : \"" + typeName + "\"} }"
						+ "\r\n");
				bodyStr.append(Json.encode(app) + "\r\n");
				i++;
			}
			HttpEntity entity = new NStringEntity(bodyStr.toString(), ContentType.APPLICATION_JSON);
			Response indexResponse = client.performRequest("POST", "/_bulk", params, entity);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.INSERT + ":" + e.getMessage(), e);
		}
		return i;
	}
	
	/**
	 * @Title: insertLog
	 * @Description: 添加日志
	 * @return void 返回类型
	 * @throws BusiException
	 */
	@SuppressWarnings("unused")
	public int insertLog(String tableName, String typeName, List<Log> pojoList) throws BusiException {
		RestClient client = getClientFactory().getClient();
		int i = 0 ;
		try {
			List<String> lists = new ArrayList<>();
			Map<String, String> params = new HashMap<String, String>();
			StringBuilder bodyStr = new StringBuilder();
			for (Log log : pojoList) {
				bodyStr.append("{ \"index\" : { \"_index\" : \"" + tableName + "\", \"_type\" : \"" + typeName + "\"} }"
						+ "\r\n");
					bodyStr.append(Json.encode(log) + "\r\n");
					i++;
			}
			HttpEntity entity = new NStringEntity(bodyStr.toString(), ContentType.APPLICATION_JSON);
			Response indexResponse = client.performRequest("POST", "/_bulk",params,entity);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.INSERT + ":" + e.getMessage(), e);
		}
		return i;
	}
	
	/**
	 * @Title:
	 * @Description: 操作记录-删除-根据条件删除
	 * @param param
	 *            删除的条件
	 * @return void 返回类型
	 * @throws BusiException
	 */
	@SuppressWarnings({ "unused", "rawtypes" })
	public void deleteByParam(String tableName, String typeName,String id, Map param) throws BusiException {
		try {
			RestClient client = getClientFactory().getClient();
			//由于版本internal控制不支持将值0作为有效的版本号，因此版本等于零的文档无法使用删除， _delete_by_query并且将会使请求失败。
//			String url = "/" + tableName + "/" + typeName + "/_delete_by_query";
			String url = "/" + tableName + "/" + typeName + "/" + id;
			String paramStr = Json.encode(param);
			JsonObject match = new JsonObject();
			match.put("match", paramStr).toString();
			JsonObject query = new JsonObject();
			query.put("query", match);
			Map<String, String> params = new HashMap<String, String>();
			HttpEntity entity = new NStringEntity(query.toString(), ContentType.APPLICATION_JSON);
			Response response = client.performRequest("DELETE", url, params, entity);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.DELETE + ":" + e.getMessage(), e);
		}
	}

	/**
	 * 删除索引
	 *
	 * @param indexName
	 * @throws BusiException
	 */
	@SuppressWarnings("unused")
	public void deleteTable(String indexName) throws BusiException {
		if (!isIndexExists(indexName)) {
		} else {
			RestClient client = getClientFactory().getClient();
			String url = "/" + indexName;
			try {
				Response response = client.performRequest("DELETE", url);
			} catch (IOException e) {
				throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.DELETE + ":" + e.getMessage(), e);
			}
		}
	}

	/**
	 * 判断索引是否存在 传入参数为索引库名称
	 *
	 * @param indexName
	 * @return
	 * @throws BusiException
	 */
	public boolean isIndexExists(String indexName) throws BusiException {
		boolean flag = false;
		try {
			RestClient client = getClientFactory().getClient();
			String url = "/" + indexName;
			Response response = client.performRequest("GET", url);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() == 200) {
				flag = true;
			} else if (status.getStatusCode() == 404) {
				flag = false;
			}
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.QUERY + ":" + e.getMessage(), e);
		}
		return flag;
	}

	/**
	 * @param tableName
	 * @param typeName
	 * @param id
	 * @param pojo
	 * @return String 返回类型
	 * @throws BusiException
	 * @throws BusiException
	 * @Title: updateAppName
	 * @Description: 日志操作记录-修改
	 */
	@SuppressWarnings("unused")
	public <E> void update(String tableName, String typeName, String id, E pojo) throws BusiException {
		try {
			RestClient client = getClientFactory().getClient();
			String url = "/" + tableName + "/" + typeName + "/" + id;
			String bodyStr = Json.encode(pojo);
			HttpEntity entity = new NStringEntity(bodyStr, ContentType.APPLICATION_JSON);
			Map<String, String> params = new HashMap<String, String>();
			Response indexResponse = client.performRequest("PUT", url, params, entity);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.UPDATE + ":" + e.getMessage(), e);
		}
	}
}
