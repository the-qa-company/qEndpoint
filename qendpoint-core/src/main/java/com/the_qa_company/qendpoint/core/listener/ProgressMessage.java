package com.the_qa_company.qendpoint.core.listener;

import java.util.Objects;

/**
 * Lazy progress message that defers string building (including {@code toString}
 * on arguments) until {@link #render()} is called.
 */
public final class ProgressMessage {
	private final String template;
	private final Object[] args;

	private ProgressMessage(String template, Object[] args) {
		this.template = Objects.requireNonNull(template, "template");
		this.args = args;
	}

	public static ProgressMessage literal(String message) {
		return new ProgressMessage(Objects.requireNonNull(message, "message"), null);
	}

	public static ProgressMessage format(String template, Object... args) {
		Objects.requireNonNull(template, "template");
		if (args == null || args.length == 0) {
			return new ProgressMessage(template, null);
		}
		return new ProgressMessage(template, args);
	}

	public String render() {
		if (args == null || args.length == 0) {
			return template;
		}

		StringBuilder message = new StringBuilder(template.length() + args.length * 8);
		int templateIndex = 0;
		int argIndex = 0;
		while (true) {
			int placeholderIndex = template.indexOf("{}", templateIndex);
			if (placeholderIndex < 0) {
				message.append(template, templateIndex, template.length());
				break;
			}
			message.append(template, templateIndex, placeholderIndex);
			if (argIndex < args.length) {
				message.append(String.valueOf(args[argIndex++]));
			} else {
				message.append("{}");
			}
			templateIndex = placeholderIndex + 2;
		}

		return message.toString();
	}
}
