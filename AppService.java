package com.pcitc.imp.bizlog.bll.itf;

import java.util.List;

import org.springframework.stereotype.Service;

import com.pcitc.imp.bizlog.bll.entity.App;
import com.pcitc.imp.bizlog.exception.BusiException;

/**
 * @ClassName: AppService 
 * @Description: 业务应用集合
 * @author 
 * @date 2016年9月5日 上午8:48:34 
 *
 */
@Service
public interface AppService {
	
	/**
	 * @Title: getAppByCode 
	 * @Description: 日志操作记录-查询-条件查询code 
	 * @param param
	 * @param code
	 * @throws BusiException
	 * @return List<App>    返回类型 
	 */
	public List<App> getAppByCode(String code) throws BusiException;
	
	/**
	 * @throws BusiException 
	 * @Title: addApp 
	 * @Description: 日志操作记录-添加
	 * @param param
	 * @param appModel
	 * @return void    返回类型 
	 * @throws BusiException
	 */
	public int addApp(List<App> etAppList) throws BusiException;
	
	/**
	 * @Title: DeleteApp 
	 * @Description: 日志操作记录-删除-根据条件删除
	 * @param param
	 * @param code
	 * @return void    返回类型 
	 * @throws BusiException
	 */
	public void deleteApp(String code) throws BusiException;
	
	/**
	 * @Title: updateAppName 
	 * @Description: 日志操作记录-修改
	 * @param param
	 * @param code
	 * @param appModel
	 * @throws BusiException
	 * @return String    返回类型 
	 * @throws BusiException
	 */
	public void updateAppName(String code,List<App> appEntitys) throws BusiException;

	/**
	 * @Title: getAppsByPage 
	 * @Description:分页查询 +codeList 查询
	 * @param param
	 * @param skip
	 * @param top
	 * @param codeList
	 * @throws BusiException
	 * @return List<App>    返回类型 
	 */
	public List<App> getAppsByPage(List<Integer> countList,String skip, String top, String codeList) throws BusiException;
	
	public List<App> getAppsByPage(String skip, String top, String codeList) throws BusiException;
}
