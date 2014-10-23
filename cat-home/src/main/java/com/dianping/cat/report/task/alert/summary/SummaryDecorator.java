package com.dianping.cat.report.task.alert.summary;

import java.io.StringWriter;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;

import com.dianping.cat.Cat;

import freemarker.template.Configuration;
import freemarker.template.Template;

public abstract class SummaryDecorator implements Initializable {

	public Configuration m_configuration;

	public String generateHtml(Map<Object, Object> dataMap) {
		StringWriter sw = new StringWriter(5000);

		try {
			Template t = m_configuration.getTemplate(getTemplateAddress());
			t.process(dataMap, sw);
		} catch (Exception e) {
			Cat.logError(e);
		}
		return sw.toString();
	}

	@Override
	public void initialize() throws InitializationException {
		m_configuration = new Configuration();
		m_configuration.setDefaultEncoding("UTF-8");
		try {
			m_configuration.setClassForTemplateLoading(this.getClass(), "/freemaker");
		} catch (Exception e) {
			Cat.logError(e);
		}
	}

	protected abstract String getTemplateAddress();

	protected abstract String getID();
}