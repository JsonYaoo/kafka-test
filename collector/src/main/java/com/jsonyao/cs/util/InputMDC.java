package com.jsonyao.cs.util;

import org.slf4j.MDC;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Log4j2 MDC线程局部变量
 */
@Component
public class InputMDC implements EnvironmentAware {

    private static Environment environment;

    /**
     * 获取并设置environment
     * @param environment
     */
    @Override
    public void setEnvironment(Environment environment) {
        InputMDC.environment = environment;
    }

    /**
     * 设置MDC线程局部变量
     */
    public static void putMDC(){
        MDC.put("hostName", NetUtil.getLocalHostName());
        MDC.put("ip", NetUtil.getLocalIp());
        MDC.put("applicationName", InputMDC.environment.getProperty("spring.application.name"));
    }
}
