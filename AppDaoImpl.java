package com.pcitc.imp.bizlog.dal.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.ResponseException;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.pcitc.imp.bizlog.dal.dao.AppDao;
import com.pcitc.imp.bizlog.dal.dao.base.ElasticSearchDao;
import com.pcitc.imp.bizlog.dal.pojo.App;
import com.pcitc.imp.bizlog.exception.BusiException;
import com.pcitc.imp.bizlog.util.CheckPrompt;
import com.pcitc.imp.bizlog.util.CheckUtil;
import com.pcitc.imp.bizlog.util.ErrorCodeEnum;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

/**
 * app的持久化
 * 
 * @author haiwen.wang
 *
 */
@Service
@Component
public class AppDaoImpl extends ElasticSearchDao implements AppDao {

	public static String APPREGISTET_INDEX = "appregister";
	public static String COMMUNAL_TYPE = "article";
	public static String CODE = "code";
	public static String NAME = "name";

	/**
	 * 创建appregister索引
	 * 
	 * @throws BusiException
	 */
	public void creatAppRegister() throws BusiException {
		String url = "/" + AppDaoImpl.APPREGISTET_INDEX;
		JsonObject code = new JsonObject();
		code.put("type", "string");
		code.put("index", "not_analyzed");
		JsonObject name = new JsonObject();
		name.put("type", "string");
		name.put("index", "analyzed");
		JsonObject properties = new JsonObject();
		properties.put("code", code);
		properties.put("name", name);
		JsonObject article = new JsonObject();
		article.put("properties", properties);
		JsonObject mappings = new JsonObject();
		mappings.put("article", article);
		JsonObject body = new JsonObject();
		body.put("mappings", mappings);

		creatIndex(url, body.toString());
	}

	/**
	 * 创建appregister索引 创建logs索引
	 * 
	 * @throws BusiException
	 */
	public void creatLogIndex(List<App> apps, String appCode) throws BusiException {
		String url = null;
		if (CheckUtil.checkStringIsNotNull(appCode)) {
			for (int i = 0; i < appCode.length(); i++) {
				url = "/" + appCode;
				JsonObject topic = new JsonObject();
				topic.put("type", "string");
				topic.put("store", "yes");
				topic.put("analyzer", "ik_max_word");

				JsonObject usercode = new JsonObject();
				usercode.put("type", "string");
				usercode.put("store", "yes");
				usercode.put("analyzer", "ik_max_word");

				JsonObject username = new JsonObject();
				username.put("type", "string");
				username.put("store", "yes");
				username.put("analyzer", "ik_max_word");

				JsonObject operation = new JsonObject();
				operation.put("type", "string");
				operation.put("store", "yes");
				operation.put("analyzer", "ik_max_word");

				JsonObject operobj = new JsonObject();
				operobj.put("type", "string");
				operobj.put("store", "yes");
				operobj.put("analyzer", "ik_max_word");

				JsonObject description = new JsonObject();
				description.put("type", "string");
				description.put("store", "yes");
				description.put("analyzer", "ik_max_word");

				JsonObject properties = new JsonObject();
				properties.put("topic", topic);
				properties.put("usercode", usercode);
				properties.put("username", username);
				properties.put("operation", operation);
				properties.put("operobj", operobj);
				properties.put("description", description);

				JsonObject article = new JsonObject();
				article.put("properties", properties);
				JsonObject mappings = new JsonObject();
				mappings.put("article", article);
				JsonObject body = new JsonObject();
				body.put("mappings", mappings);

				creatIndex(url, body.toString());
			}
		} else {
			for (int i = 0; i < apps.size(); i++) {
				url = "/" + apps.get(i).getCode();
				JsonObject topic = new JsonObject();
				topic.put("type", "string");
				topic.put("store", "yes");
				topic.put("analyzer", "ik_max_word");

				JsonObject usercode = new JsonObject();
				usercode.put("type", "string");
				usercode.put("store", "yes");
				usercode.put("analyzer", "ik_max_word");

				JsonObject username = new JsonObject();
				username.put("type", "string");
				username.put("store", "yes");
				username.put("analyzer", "ik_max_word");

				JsonObject operation = new JsonObject();
				operation.put("type", "string");
				operation.put("store", "yes");
				operation.put("analyzer", "ik_max_word");

				JsonObject operobj = new JsonObject();
				operobj.put("type", "string");
				operobj.put("store", "yes");
				operobj.put("analyzer", "ik_max_word");

				JsonObject description = new JsonObject();
				description.put("type", "string");
				description.put("store", "yes");
				description.put("analyzer", "ik_max_word");

				JsonObject properties = new JsonObject();
				properties.put("topic", topic);
				properties.put("usercode", usercode);
				properties.put("username", username);
				properties.put("operation", operation);
				properties.put("operobj", operobj);
				properties.put("description", description);

				JsonObject article = new JsonObject();
				article.put("properties", properties);
				JsonObject mappings = new JsonObject();
				mappings.put("article", article);
				JsonObject body = new JsonObject();
				body.put("mappings", mappings);

				creatIndex(url, body.toString());
			}
		}
	}

	/**
	 * @Title: getAppByCode
	 * @Description: app查询-条件查询code
	 * @param code
	 * @throws BusiException
	 * @return List<App> 返回类型
	 */
	public List<App> getAppByCode(String code) throws BusiException {

		List<String> ret;
		List<App> result = new ArrayList<>();
		try {
			ret = queryByField(APPREGISTET_INDEX, App.FIELD_CODE, code);
			if (!ret.isEmpty()) {
				for (String jsonStr : ret) {
					App app = convert2App(jsonStr);
					result.add(app);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private App convert2App(String jsonStr) {
		App app = Json.decodeValue(jsonStr, App.class);
		return app;
	}

	/**
	 * 根据appcode查询
	 */
	public boolean queryByParam(String appCode) throws BusiException {
		boolean flag;
		List<String> result = queryByField(APPREGISTET_INDEX, App.FIELD_CODE, appCode);
		if (result == null || result.isEmpty()) {
			flag = false;
		} else {
			flag = true;
		}
		return flag;
	}

	/**
	 * 校验 是否存在 appCode
	 */
	public boolean queryLogByAppCode(String appCode) throws BusiException {
		boolean flag = false;
		flag = isIndexExists(appCode);
		if (flag) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * @Title: insert
	 * @Description: 添加
	 * @return void 返回类型
	 * @throws BusiException
	 */
	public int insertApp(List<App> apps) throws BusiException {
		int insertApp = insertApp(APPREGISTET_INDEX, COMMUNAL_TYPE, apps);
		creatLogIndex(apps, "");
		return insertApp;
	}

	/**
	 * @Title: deleteApp
	 * @Description: 日志操作记录-删除-根据条件删除
	 * @param code
	 * @return void 返回类型
	 * @throws BusiException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void deleteApp(String code) throws BusiException {
		Map param = new HashMap();
		param.put(App.FIELD_CODE, code);
		List<String> result = queryByFieldUpdate(APPREGISTET_INDEX, App.FIELD_CODE, code);
		String id = result.get(1);
		deleteByParam(APPREGISTET_INDEX, COMMUNAL_TYPE, id, param);
	}

	/**
	 * @Title: updateAppName
	 * @Description: 日志操作记录-修改
	 * @param code
	 * @param app
	 * @throws BusiException
	 * @return String 返回类型
	 * @throws BusiException
	 *             TODO 优化为按照code直接更新数据
	 */
	public void updateAppName(String code, App app) throws BusiException {
		try {
			// 先根据code查询id
			List<String> result = queryByFieldUpdate(APPREGISTET_INDEX, App.FIELD_CODE, code);
			String id = result.get(1);
			App apps = new App();
			apps.setCode(code);
			apps.setName(app.getName());
			// 然后根据id更新名称
			update(APPREGISTET_INDEX, COMMUNAL_TYPE, id, apps);
		} catch (BusiException e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.UPDATE + e.getMessage(), e);
		}
	}

	/**
	 * 分页查询
	 * 
	 * @param skip
	 * @param top
	 * @return
	 * @throws BusiException
	 */
	public List<com.pcitc.imp.bizlog.dal.pojo.App> getAppsByPage(List<Integer> countList, String skip, String top,
			String codeList) throws BusiException {
		List<String> ret = new ArrayList<>();
		List<App> result = new ArrayList<App>();
//		int t = Integer.parseInt(top);
//		int s = t + 1;
//		int sk = Integer.parseInt(skip);
		if (CheckUtil.checkStringIsNull(skip) && CheckUtil.checkStringIsNull(top)
				&& CheckUtil.checkStringIsNull(codeList)) {
			ret = queryAllApp(APPREGISTET_INDEX);
		} else {
			ret = queryAppByCon(APPREGISTET_INDEX,skip,top, codeList);
		}
		if (!ret.isEmpty()) {
			int parseInt = Integer.parseInt(ret.get(0).toString());
			countList.add(parseInt);
			for (int i = 0; i < ret.size(); i++) {
				if (i > 0) {
					App app = convert2App(ret.get(i));
					result.add(app);
				}
			}
		}

		return result;
	}

	public List<com.pcitc.imp.bizlog.dal.pojo.App> getAppsByPage(String skip, String top, String codeList)
			throws BusiException {
		List<String> ret = new ArrayList<>();
		List<App> result = new ArrayList<App>();
		if (CheckUtil.checkStringIsNull(skip) && CheckUtil.checkStringIsNull(top)
				&& CheckUtil.checkStringIsNull(codeList)) {
			ret = queryAllApp(APPREGISTET_INDEX);
		} else {
			ret = queryAppByCon(APPREGISTET_INDEX, skip, top, codeList);
		}
		if (!ret.isEmpty()) {
			for (String jsonStr : ret) {
				String[] strs = jsonStr.split(",");
				for (int i = 0; i < strs.length; i++) {
					if (i == 0) {

					} else if(i == 1){
						App app = convert2App(jsonStr);
						result.add(app);
					}
				}
			}
		}
		return result;
	}

	/**
	 * @Title: getApps
	 * @Description: 日志操作记录-查询-查询所有数据
	 * @return CommonResult 返回类型
	 */
	public List<String> queryAllApp(String indexName) throws BusiException {
		List<String> result;
		try {
			result = queryAll(indexName);
		} catch (ResponseException e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.APP_NOT_EXIST, e);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, e.getMessage(), e);
		}
		return result;
	}

	/**
	 * @Title: getApps
	 * @Description: 应用操作记录-查询-按编码列表
	 * @return CommonResult 返回类型
	 */
	public List<String> queryAppByCon(String indexName, String skip, String top, String codeList) throws BusiException {
		List<String> result;
		try {
			Map<String, Param> paramMap = new HashMap<>();
			Param param = new Param();
			param.setType(DataType.string);
			param.setQueryType("should");
			param.setMatchType("term");
			List<String> value = new ArrayList<>();
			if (!CheckUtil.checkStringIsNull(codeList)) {
				String[] strs = codeList.split(",");
				for (int i = 0; i < strs.length; i++) {
					value.add(strs[i]);
				}
				param.setValue(value);
				paramMap.put(CODE, param);
			}
			result = queryByCon(indexName, paramMap, skip, top, null);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, e.getMessage(), e);
		}
		return result;
	}
}
