package com.the_qa_company.qendpoint.core.util.debug;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Tool to register debug point
 *
 * @author Antoine Willerval
 */
public class DebugInjectionPointManager {
	/**
	 * Policy to keep the action
	 */
	public enum DebugPolicy {
		/**
		 * Run the action once and delete it
		 */
		RUN_ONCE(0),
		/**
		 * Run the action until it throws, will clear it at the end
		 */
		RUN_THROW(1),
		/**
		 * Run the action until the end
		 */
		RUN_END(2),
		/**
		 * Keep the action after the end
		 */
		NO_DELETE(3);

		private final int id;

		DebugPolicy(int id) {
			this.id = id;
		}

		public boolean deleteAfterEnd() {
			return id <= RUN_END.id;
		}

		public boolean deleteAfterThrow() {
			return id <= RUN_THROW.id;
		}

		public boolean deleteAfterRun() {
			return id <= RUN_ONCE.id;
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(DebugInjectionPointManager.class);
	private static final DebugInjectionPointManager instance = new DebugInjectionPointManager();

	/**
	 * @return the instance of the manager
	 */
	public static DebugInjectionPointManager getInstance() {
		return instance;
	}

	/**
	 * a debug point
	 *
	 * @param <E> type of the point
	 */
	public class DebugInjectionPoint<E> {
		private final int id;
		private final String name;
		private Throwable anyException;

		private DebugInjectionPoint(int id, String name) {
			this.id = id;
			this.name = name;
		}

		public String getName() {
			return name;
		}

		/**
		 * register an action, the {@link DebugPolicy#RUN_THROW} policy will be used
		 *
		 * @param action new action
		 */
		public void registerAction(DebugInjectionAction<E> action) {
			registerAction(DebugPolicy.RUN_THROW, action);
		}

		/**
		 * register an action
		 *
		 * @param policy policy to delete the action
		 * @param action new action
		 */
		public void registerAction(DebugPolicy policy, DebugInjectionAction<E> action) {
			if (actions.get(id) != null) {
				logger.warn("a previous action was already specified in the point manager for {}", getName());
			}
			actions.set(id, new ActionInfo<>(action, policy));
		}

		/**
		 * Run the action
		 * @param obj this
		 */
		@SuppressWarnings("unchecked")
		public void runAction(E obj) {
			ActionInfo<E> act = (ActionInfo<E>) actions.get(id);
			if (act == null) {
				return;
			}
			if (act.policy.deleteAfterRun()) {
				actions.set(id, null);
			}
			try {
				act.handle(obj);
			} catch (Throwable t) {
				if (act.policy.deleteAfterThrow()) {
					actions.set(id, null);
				}
				anyException = t;
			}
		}

		/**
		 * throw any exception result
		 * @throws Exception exception
		 * @throws Error error
		 */
		public void throwExceptionResult() throws Exception, Error {
			ActionInfo<?> act = actions.get(id);
			if (act != null && act.policy.deleteAfterEnd()) {
				actions.set(id, null);
			}
			if (anyException instanceof Exception e) {
				throw e;
			}
			if (anyException instanceof Error err) {
				throw err;
			}
			if (anyException != null) {
				throw new Error(anyException);
			}
		}
	}

	private record ActionInfo<E>(DebugInjectionAction<E> action, DebugPolicy policy) {
		/**
		 * handle a class
		 *
		 * @param e current object
		 */
		void handle(E e) throws Exception {
			action.handle(e);
		}
	}

	public interface DebugInjectionAction<E> {
		/**
		 * handle a class
		 *
		 * @param e current object
		 */
		void handle(E e) throws Exception;
	}

	private DebugInjectionPointManager() {
	}

	private final List<ActionInfo<?>> actions = new ArrayList<>();

	/**
	 * register an injection point
	 *
	 * @param name object to name the point, a {@link #toString()} will be applied
	 * @param <E>  debug point type
	 * @return debug point
	 */
	public synchronized <E> DebugInjectionPoint<E> registerInjectionPoint(Object name) {
		return registerInjectionPoint(Objects.requireNonNull(name).toString());
	}

	/**
	 * register an injection point
	 *
	 * @param name point name
	 * @param <E>  debug point type
	 * @return debug point
	 */
	public synchronized <E> DebugInjectionPoint<E> registerInjectionPoint(String name) {
		DebugInjectionPoint<E> ip = new DebugInjectionPoint<>(actions.size(), Objects.requireNonNull(name));
		// ensure the size
		actions.add(null);
		return ip;
	}
}
