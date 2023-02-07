package com.the_qa_company.qendpoint.core.search;

import com.the_qa_company.qendpoint.core.hdt.HDT;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Query tool factory, to register a new query tool, you need to add into the
 * META-INF/services directory a file named
 * {@literal com.the_qa_company.qendpoint.core.search.HDTQueryToolFactory} with
 * a line containing the name of the class with the implementation. You have 2
 * different QueryTool, generic and not generic, respectively get with
 * {@link #newGenericQueryTool(HDT)} and {@link #newQueryTool(HDT)}. To say if
 * you can create a generic query tool, {@link #hasGenericTool()} should return
 * true. If multiple generic tool are implemented, the first implementation is
 * used.
 * <p>
 * A generic query tool is implemented in the CORE module.
 *
 * @author Antoine Willerval
 */
public abstract class HDTQueryToolFactory {
	private static final ServiceLoader<HDTQueryToolFactory> LOADER = ServiceLoader.load(HDTQueryToolFactory.class);
	private static List<HDTQueryToolFactory> factory;

	/**
	 * @return factory for the HDTSearch
	 */
	static List<HDTQueryToolFactory> getFactories() {
		if (factory != null) {
			return factory;
		}
		List<HDTQueryToolFactory> list = LOADER.stream().map(ServiceLoader.Provider::get).collect(Collectors.toCollection(ArrayList::new));
		list.add(new HDTQueryToolFactoryImpl());
		return factory = list;
	}

	/**
	 * create a query tool from an HDT
	 *
	 * @param hdt hdt
	 * @return HDTQueryTool
	 */
	public static HDTQueryTool createQueryTool(HDT hdt) {
		Objects.requireNonNull(hdt, "hdt can't be null!");
		HDTQueryToolFactory generic = null;
		for (HDTQueryToolFactory toolFactory : getFactories()) {
			HDTQueryTool tool = toolFactory.newGenericQueryTool(hdt);
			if (tool != null) {
				return tool;
			}
			if (toolFactory.hasGenericTool()) {
				generic = toolFactory;
			}
		}
		if (generic == null) {
			throw new IllegalArgumentException(
					"Can't find service to handle " + hdt.getClass() + "! Did you add the core to the libraries?");
		}

		HDTQueryTool tool = generic.newGenericQueryTool(hdt);
		assert tool != null : "generic tool can't be null!";
		return tool;
	}

	/**
	 * create query tool from an HDT, this method should be specific to the
	 * implemented HDT and should not be generic, use
	 * {@link #newGenericQueryTool(HDT)} to create generic HDT query tool.
	 *
	 * @param hdt hdt
	 * @return HDTQueryTool or null if the HDT can't be used
	 */
	public abstract HDTQueryTool newQueryTool(HDT hdt);

	/**
	 * @return if this factory has a generic tool
	 */
	public abstract boolean hasGenericTool();

	/**
	 * create a generic query tool from an HDT, this tool shouldn't be linked
	 * with the HDT implementation
	 *
	 * @param hdt hdt
	 * @return HDTQueryTool
	 */
	public abstract HDTQueryTool newGenericQueryTool(HDT hdt);
}
