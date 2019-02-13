package com.pcitc.imp.bizlog.service.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import com.pcitc.imp.bizlog.bll.itf.AppService;
import com.pcitc.imp.bizlog.exception.BusiException;
import com.pcitc.imp.bizlog.service.model.App;
import com.pcitc.imp.bizlog.util.CheckPrompt;
import com.pcitc.imp.bizlog.util.CheckUtil;
import com.pcitc.imp.bizlog.util.ErrorCodeEnum;
import com.pcitc.imp.common.handler.BaseHandler;
import com.pcitc.imp.common.model.Pagination;

import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import pcitc.imp.common.ettool.utils.ObjectConverter;
import pcitc.imp.common.ettool.utils.RestfulTool;

/**
 * Created by pcitc on 2016/12/27.
 */
@Controller
public class AppHandler extends BaseHandler {
	
	@Autowired
	private AppService appService;

    /**
     * 新增应用
     *
     * @param routingContext
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public void addApp(RoutingContext routingContext) {
        try {
            List<com.pcitc.imp.bizlog.service.model.App> listApp = RestfulTool.toResourceRep(routingContext.getBodyAsString(),
            		com.pcitc.imp.bizlog.service.model.App.class);
            // 空格校验
            List<App> apps = getStringNoBlank(listApp);
            // 非空校验
//            checkInput(apps);
            for (App app : apps) {
                app.setHref(routingContext.normalisedPath() + "/" + app.getCode());
            }
            List appEntitys = ObjectConverter.listConverter(apps, com.pcitc.imp.bizlog.bll.entity.App.class);
            // 创建app
            Vertx vertx = routingContext.vertx();
            vertx.executeBlocking(future -> {
                try {
                	Integer addApp = appService.addApp(appEntitys);
                    future.complete(addApp.toString());
                } catch (Exception e) {
                    future.fail(e);
                }
            }, res -> {
                String collecion = null;
                if (res.failed()) {
                    collecion = buildErrorCollection(routingContext, (BusiException) res.cause());
                } else if (res.succeeded()) {
                	try {
                		Object result = res.result();
                        returnCollection(routingContext, result.toString());
                    } catch (Exception e) {
                        collecion = buildErrorCollection(routingContext, e);
                        returnCollection(routingContext, collecion);
                    }
                }
                returnCollection(routingContext, collecion);
            });
        } catch (BusiException e) {
            String collecion = buildErrorCollection(routingContext, e);
            returnCollection(routingContext, collecion);
        } catch (Exception e) {
            String collecion = buildErrorCollection(routingContext, e);
            returnCollection(routingContext, collecion);
        }
    }

    /**
     * 查询应用
     *
     * @param routingContext
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public void getApps(RoutingContext routingContext) {

        String skip = (routingContext.request().getParam("$skip") == null ? null : routingContext.request().getParam("$skip").trim());
        String top = (routingContext.request().getParam("$top") == null ? null : routingContext.request().getParam("$top").trim());
        String codeList = (routingContext.request().getParam("$codeList") == null ? null : routingContext.request().getParam("$codeList").trim());
        try {
            // 输入校验
            String codes = checkInput(skip, top, codeList);
            Pagination pagination = new Pagination();
            // 查询app
            Vertx vertx = routingContext.vertx(); 
            vertx.executeBlocking(future -> {
                List<com.pcitc.imp.bizlog.bll.entity.App> appList = null;
                try {
                	List<Integer> countList = new ArrayList();
                    
                    if(routingContext.request().getParam("$skip") != null && routingContext.request().getParam("$top") != null){
                    	appList = appService.getAppsByPage(countList,skip, top, codes);
                    	if (!appList.isEmpty()) {
                    		pagination.setRecordCount(Long.valueOf(countList.get(0).toString()));
                    	}
                    }else{
                    	appList = appService.getAppsByPage(skip, top, codes);
                    }
                    future.complete(appList);
                } catch (BusiException e) {
                    future.fail(e);
                }
            }, res -> {
                String collecion = null;
                if (res.failed()) {
                    collecion = buildErrorCollection(routingContext, (BusiException) res.cause());
                    returnCollection(routingContext, collecion);
                } else if (res.succeeded()) {
                    try {
                        List<App> listApp = ObjectConverter.listConverter((List<com.pcitc.imp.bizlog.bll.entity.App>) res.result(), App.class);
                        collecion = RestfulTool.buildCollection(listApp, pagination, routingContext.request().absoluteURI(), App.class);
                        returnCollection(routingContext, collecion);
                    } catch (Exception e) {
                        collecion = buildErrorCollection(routingContext, e);
                        returnCollection(routingContext, collecion);
                    }
                }
            });
        } catch (BusiException e) {
            String collecion = buildErrorCollection(routingContext, e);
            returnCollection(routingContext, collecion);
        }
    }

    /**
     * 根据编码查询应用
     *
     * @param routingContext
     */
    @SuppressWarnings("unchecked")
	public void getAppByCode(RoutingContext routingContext) {
        try {
            String codetrim = routingContext.request().getParam("code").trim();
            // 输入校验
            checkInput(codetrim);
            // 查询app
            Vertx vertx = routingContext.vertx();
            vertx.executeBlocking(future -> {
                List<com.pcitc.imp.bizlog.bll.entity.App> appList = null;
                try {
                    appList = appService.getAppByCode(codetrim);
                    future.complete(appList);
                } catch (BusiException e) {
                    future.fail(e);
                }
            }, res -> {
                String collecion = null;
                if (res.failed()) {
                    collecion = buildErrorCollection(routingContext, (BusiException) res.cause());
                    returnCollection(routingContext, collecion);
                } else if (res.succeeded()) {
                    try {
                        List<App> listApp = ObjectConverter.listConverter((List<com.pcitc.imp.bizlog.bll.entity.App>) res.result(), App.class);
                        collecion = RestfulTool.buildCollection(listApp, routingContext.request().absoluteURI(), App.class);
                        returnCollection(routingContext, collecion);
                    } catch (Exception e) {
                        collecion = buildErrorCollection(routingContext, e);
                        returnCollection(routingContext, collecion);
                    }
                }
            });
        } catch (BusiException e) {
            String collecion = buildErrorCollection(routingContext, e);
            returnCollection(routingContext, collecion);
        }
    }

    /**
     * 更新应用的名称
     * 不支持批更新
     *
     * @param routingContext
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public void updateAppName(RoutingContext routingContext) {
        try {
            List<com.pcitc.imp.bizlog.service.model.App> listApp = RestfulTool.toResourceRep(routingContext.getBodyAsString(),
            		com.pcitc.imp.bizlog.service.model.App.class);
            // 空格校验
            listApp = getStringNoBlank(listApp);
            // 非空校验
            checkInputUp(listApp);
            List appEntity = ObjectConverter.listConverter(listApp, com.pcitc.imp.bizlog.bll.entity.App.class);
            // 更新app
            Vertx vertx = routingContext.vertx();
            vertx.executeBlocking(future -> {
                try {
                	appService.updateAppName(routingContext.request().getParam("code"), appEntity);
                    future.complete();
                } catch (BusiException e) {
                    future.fail(e);
                }
            }, res -> {
                String collecion = null;
                if (res.failed()) {
                    collecion = buildErrorCollection(routingContext, (BusiException) res.cause());
                } else if (res.succeeded()) {
//                    collecion = "ok";
                }
                returnCollection(routingContext, collecion);
            });
        } catch (BusiException e) {
            String collecion = buildErrorCollection(routingContext, e);
            returnCollection(routingContext, collecion);
        } catch (Exception e) {
            String collecion = buildErrorCollection(routingContext, e);
            returnCollection(routingContext, collecion);
        }
    }

    /**
     * 删除应用
     *
     * @param routingContext
     */
    public void deleteApp(RoutingContext routingContext) {
        String code = routingContext.request().getParam("code");
        String codeVal = (code == null ? null : code.trim());
        try {
            checkInput(codeVal);
            // 更新app
            Vertx vertx = routingContext.vertx();
            vertx.executeBlocking(future -> {
                try {
                	appService.deleteApp(codeVal);
                    future.complete();
                } catch (BusiException e) {
                    future.fail(e);
                }
            }, res -> {
                String collecion = null;
                if (res.failed()) {
                    collecion = buildErrorCollection(routingContext, (BusiException) res.cause());
                } else if (res.succeeded()) {
//                    collecion = "ok";
                }
                returnCollection(routingContext, collecion);
            });
        } catch (BusiException e) {
            String collecion = buildErrorCollection(routingContext, e);
            returnCollection(routingContext, collecion);
        }
    }

    /**
     * @param code
     * @return void    返回类型
     * @throws BusiException
     * @Title: checkInput
     * @Description: 查询校验code
     */
    public void checkInput(String code) throws BusiException {
        if (CheckUtil.checkStringIsNull(code)) {
            throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.APP_CODE_NULL);
        }
        Matcher checkMatcher = CheckUtil.checkMatcher(code);
        if (checkMatcher.find() == false) {
            throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.CHECKMATCHER);
        }
    }

    /**
     * 新增前数据校验
     *
     * @param appModels
     * @throws BusiException
     */
    public void checkInput(List<App> appModels) throws BusiException {
        for (App appModel : appModels) {
            if (CheckUtil.checkStringIsNull(appModel.getCode())) {
                throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.APP_CODE_NULL+appModel.getCode());
            }
            if (CheckUtil.checkStringIsNull(appModel.getName())) {
                throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.APP_NAME_NULL+appModel.getName());
            }
            Matcher checkMatcher = CheckUtil.checkMatcher(appModel.getCode());
            if (checkMatcher.find() == false) {
                throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.CHECKMATCHER+appModel.getCode());
            }
            if (CheckUtil.characterFilter(appModel.getName())) {
                throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.APP_NAME_CHECKMATCHER+appModel.getName());
            }
        }
    }

    /**
     * @param appModel
     * @return App    返回类型
     * @Title: getStringNoBlank
     * @Description: 校验前后空格
     */
    public List<App> getStringNoBlank(List<App> appModel) {
        for (App app : appModel) {
            String code = null;
            String name = null;
            if (app.getCode() != null && !"".equals(app.getCode())) {
                Matcher m = CheckUtil.checkNoBlank(app.getCode());
                code = m.replaceAll("");
            }
            if (app.getName() != null && !"".equals(app.getName())) {
                Matcher m = CheckUtil.checkNoBlank(app.getName());
                name = m.replaceAll("");
            }
            app.setCode(code);
            app.setName(name);
        }
        return appModel;
    }

    /**
     * @param appModel
     * @throws BusiException
     * @Title: checkInputUp
     * @Description: 修改时非空校验
     */
    public static void checkInputUp(List<App> appModel) throws BusiException {
        for (App app : appModel) {
            if (CheckUtil.checkStringIsNull(app.getName())) {
                throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.APP_NAME_NULL);
            }
            if (CheckUtil.characterFilter(app.getName())) {
                throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.APP_NAME_CHECKMATCHER);
            }
        }
    }

    /**
     * @return
     * @Title: checkCodeList
     * @Description: 校验codeList
     */
    public static String checkCodeList(String codeList) throws BusiException {
        String resultCodeList = "";
        String[] strs = codeList.split(",");
        for (int i = 0; i < strs.length; i++) {
            // 空格校验
            String codeLists = strs[i].trim();
            // 非空校验
            boolean checkStringIsNull = CheckUtil.checkStringIsNull(codeLists);
            if (checkStringIsNull) {
                continue;
            }
            // 特殊字符判断
            if (CheckUtil.characterFilter(codeLists)) {
                continue;
            }
            resultCodeList += codeLists;
            if (i < strs.length - 1) {
                resultCodeList = resultCodeList + ",";
            }
        }
        return resultCodeList;
    }

    /**
     * @Title: checkInput
     */
    public static String checkInput(String skip, String top, String codeList) throws BusiException {
        String checkCodeList = null;
        // 特殊字符判断
        if (CheckUtil.checkStringIsNotNull(top) && CheckUtil.characterFilter(top)) {
            throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.TOP);
        }
        if (CheckUtil.checkStringIsNotNull(top) && !CheckUtil.checkDigit(top)) {
            throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.TOP_HIGHER);
        }
        if (CheckUtil.checkStringIsNotNull(top) && Integer.parseInt(top) == 0) {
            throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.TOP);
        }
        // 特殊字符判断
        if (CheckUtil.checkStringIsNotNull(skip) && CheckUtil.characterFilter(skip)) {
            throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.SKIP);
        }
        if (CheckUtil.checkStringIsNotNull(skip) && !CheckUtil.checkDigit(skip)) {
            throw new BusiException(ErrorCodeEnum.M000, CheckPrompt.SKIP);
        }

        if (CheckUtil.checkStringIsNotNull(codeList)) {
            //校验codeList
            checkCodeList = checkCodeList(codeList);
        }
        return checkCodeList;
    }
}
