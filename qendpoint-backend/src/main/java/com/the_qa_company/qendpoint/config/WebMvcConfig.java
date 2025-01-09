package com.the_qa_company.qendpoint.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

	// this is the configuration necessary for the react front-end, it redirects
	// to index.html if no match
	@Override
	public void addViewControllers(ViewControllerRegistry registry) {
		registry.addViewController("/").setViewName("forward:/index.html");
		// Single directory level - no need to exclude "api"
		registry.addViewController("/{x:[\\w\\-]+}").setViewName("forward:/index.html");
		// Multi-level directory path, need to exclude "api" on the first part
		// of the path
//    registry
//        .addViewController("/{x:^(?!api$).*$}/{y:[\\w\\-]+}")
//        .setViewName("forward:/index.html");
	}

	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry) {
		registry.addResourceHandler("swagger-ui.html").addResourceLocations("classpath:/META-INF/resources/");

		registry.addResourceHandler("/webjars/**").addResourceLocations("classpath:/META-INF/resources/webjars/");
	}
}
