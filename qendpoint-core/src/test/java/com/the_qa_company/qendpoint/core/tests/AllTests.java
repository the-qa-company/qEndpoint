package com.the_qa_company.qendpoint.core.tests;

import com.the_qa_company.qendpoint.core.compact.array.IntegerArrayTest;
import com.the_qa_company.qendpoint.core.compact.array.LongArrayTest;
import com.the_qa_company.qendpoint.core.compact.bitmap.BitSequence375Test;
import com.the_qa_company.qendpoint.core.compact.integer.VByteTest;
import com.the_qa_company.qendpoint.core.util.io.IOUtilTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ IntegerArrayTest.class, LongArrayTest.class, IntegerArrayTest.class, BitSequence375Test.class,
		IOUtilTest.class, VByteTest.class })
public class AllTests {}
