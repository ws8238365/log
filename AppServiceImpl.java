package com.pcitc.imp.bizlog.bll.itf.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.pcitc.imp.bizlog.bll.itf.AppService;
import com.pcitc.imp.bizlog.dal.dao.AppDao;
import com.pcitc.imp.bizlog.dal.dao.LogDao;
import com.pcitc.imp.bizlog.exception.BusiException;
import com.pcitc.imp.bizlog.service.model.Condition;
import com.pcitc.imp.bizlog.util.CheckPrompt;
import com.pcitc.imp.bizlog.util.CheckUtil;
import com.pcitc.imp.bizlog.util.EntityUtil;
import com.pcitc.imp.bizlog.util.ErrorCodeEnum;

import pcitc.imp.common.ettool.utils.ObjectConverter;

/**
 * @ClassName: AppServiceImpl
 * @Description: 业务应用集合
 * @author
 * @date 2016年9月5日 上午8:49:30
 */
@Service
@Component
public class AppServiceImpl implements AppService{

	@Autowired
	private AppDao appDao;
	@Autowired
	private LogDao logDao;

	/**
	 * @Title: getAppByCode
	 * @Description: app查询-条件查询code
	 * @throws BusiException
	 * @return List<App> 返回类型
	 */
	@SuppressWarnings("unchecked")
	public List<com.pcitc.imp.bizlog.bll.entity.App> getAppByCode(String code) throws BusiException {
		List<com.pcitc.imp.bizlog.dal.pojo.App> appList = null;
		List<com.pcitc.imp.bizlog.bll.entity.App> applists = new ArrayList<>();
		try {
			appList = appDao.getAppByCode(code);
			applists = ObjectConverter.listConverter(appList, com.pcitc.imp.bizlog.dal.pojo.App.class);
		} catch (BusiException e) {
			throw new BusiException(ErrorCodeEnum.M001, CheckPrompt.QUERY, e);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return applists;
	}

	/**
	 * @Title: addApp
	 * @Description: 日志操作记录-添加
	 * @return void 返回类型
	 * @param appEntitys
	 * @throws Exception 
	 */
	public int addApp(List<com.pcitc.imp.bizlog.bll.entity.App> etAppList) throws BusiException {
		List<com.pcitc.imp.bizlog.dal.pojo.App> pojoList = new ArrayList<>();
		List<BusiException> errDatas = new ArrayList<>();
		int count = etAppList.size();
		for(com.pcitc.imp.bizlog.bll.entity.App appEntity : etAppList){
			// 校验appcode是否已经存在
			if (appDao.queryByParam(appEntity.getCode())) {
				throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.APP_CODE_REPEAT);
			}
			if(etAppList != null && etAppList.size()>0){
				count--;
				checkInput(appEntity,count,errDatas);
			}
			// entity转换为pojo
			com.pcitc.imp.bizlog.dal.pojo.App appPojo;
			try {
				appPojo = EntityUtil.entity2Pojo(appEntity);
				pojoList.add(appPojo);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 创建app
		return appDao.insertApp(pojoList);
	}

	/**
	 * @Title: deleteApp @Description: 日志操作记录-删除-根据条件删除 @param code 唯一条件 @return
	 *         void 返回类型 @throws
	 */
	public void deleteApp(String code) throws BusiException {
		List<com.pcitc.imp.bizlog.dal.pojo.Log> logPojoList = null;
		Condition condition = new Condition();
		boolean logIndexExist = true;
		try {
			logPojoList = logDao.getLogsByCon(code, condition);
		} catch (BusiException e) {
			logIndexExist = false;
		}
		if (logIndexExist && !logPojoList.isEmpty()) {
			throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.APP_CODE_REPEAT_LOG);
		}
		if (logIndexExist && logPojoList.isEmpty()) {
			logDao.deleteLogs(code);
		}
		// 校验appcode是否已经存在
		if (!appDao.queryByParam(code)) {
			throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.APP_CODE_NOT_REPEAT);
		}
		appDao.deleteApp(code);
	}

	/**
	 * @Title: updateAppName
	 * @Description: 日志操作记录-修改
	 * @param code
	 * @param appEntitys
	 * @throws BusiException
	 * @return String 返回类型
	 * @throws BusiException
	 */
	@SuppressWarnings({ "unchecked", "unused" })
	public void updateAppName(String code, List<com.pcitc.imp.bizlog.bll.entity.App> appEntitys) throws BusiException {
		// 校验appcode是否已经存在
		for (com.pcitc.imp.bizlog.bll.entity.App appEntity:appEntitys) {
			if (!appDao.queryByParam(code)) {
				throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.APP_CODE_NOT_REPEAT);
			}
		}
		List<com.pcitc.imp.bizlog.dal.pojo.App> appPojoList = null;
		try {
			appPojoList = ObjectConverter.listConverter(appEntitys, com.pcitc.imp.bizlog.dal.pojo.App.class);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, e.getMessage());
		}
		appDao.updateAppName(code, appPojoList.get(0));
	}

	/**
	 * @Title: getAppsByPage
	 * @Description:分页查询 +codeList 查询
	 * @param skip
	 * @param top
	 * @throws BusiException
	 * @return List<App> 返回类型
	 */
	@SuppressWarnings("unchecked")
	public List<com.pcitc.imp.bizlog.bll.entity.App> getAppsByPage(List<Integer> countList,String skip, String top, String codes) throws BusiException {
		List<com.pcitc.imp.bizlog.dal.pojo.App> appPojo = null;
		List<com.pcitc.imp.bizlog.bll.entity.App> appEntity = new ArrayList<>();
		try {
			appPojo = appDao.getAppsByPage(countList,skip, top, codes);
			appEntity = ObjectConverter.listConverter(appPojo, com.pcitc.imp.bizlog.bll.entity.App.class);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, e.getMessage());
		}
		return appEntity;
	}
	
	@SuppressWarnings("unchecked")
	public List<com.pcitc.imp.bizlog.bll.entity.App> getAppsByPage(String skip, String top, String codes) throws BusiException {
		List<com.pcitc.imp.bizlog.dal.pojo.App> appPojo = null;
		List<com.pcitc.imp.bizlog.bll.entity.App> appEntity = new ArrayList<>();
		try {
			appPojo = appDao.getAppsByPage(skip, top, codes);
			appEntity = ObjectConverter.listConverter(appPojo, com.pcitc.imp.bizlog.bll.entity.App.class);
		} catch (Exception e) {
			throw new BusiException(ErrorCodeEnum.M001, e.getMessage());
		}
		return appEntity;
	}

	/**
	 * 创建appregister表
	 */
	public void creatAppRegister() throws BusiException{
		appDao.creatAppRegister();
	}
	
	/**
	 * @Title: checkInput 
	 * @Description: 应用批量验证新增返回信息
	 * @param appEntity
	 * @param integer
	 * @param errList
	 * @throws BusiException
	 * @return void    返回类型 
	 * @throws BusiException
	 */
	public void checkInput(com.pcitc.imp.bizlog.bll.entity.App appEntity,Integer integer,List<BusiException> errList) throws BusiException {

		if (CheckUtil.checkStringIsNull(appEntity.getCode())) {
			errList.add(new BusiException(ErrorCodeEnum.M000, appEntity.getCode()+":"+CheckPrompt.APP_CODE_NULL));
		}
		if (CheckUtil.checkStringIsNull(appEntity.getName())) {
			errList.add(new BusiException(ErrorCodeEnum.M000, appEntity.getName()+":"+CheckPrompt.APP_NAME_NULL));
		}
		Matcher checkMatcher = CheckUtil.checkMatcher(appEntity.getCode());
		if (checkMatcher.find() == false) {
			errList.add(new BusiException(ErrorCodeEnum.M000, appEntity.getCode()+":"+CheckPrompt.CHECKMATCHER));
		}
		if (CheckUtil.characterFilter(appEntity.getName())) {
			errList.add(new BusiException(ErrorCodeEnum.M000, appEntity.getName()+":"+CheckPrompt.APP_NAME_CHECKMATCHER));
		}
		if(integer ==0){
			StringBuffer sb = new StringBuffer();
			if(errList != null && errList.size()>0){
				for (BusiException busiException : errList) {
					sb.append(busiException.getMessage()+" ");
				}
				throw new BusiException(ErrorCodeEnum.M000,sb.toString());
			}
		}
	}

}
