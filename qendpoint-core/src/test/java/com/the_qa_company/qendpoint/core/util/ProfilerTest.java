package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ProfilerTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void ioTest() throws IOException, InterruptedException {
		Path root = tempDir.getRoot().toPath();

		try (Profiler profiler = new Profiler("test")) {
			profiler.setDisabled(false);
			profiler.pushSection("tests1");
			{
				profiler.pushSection("tests1s1");
				{
					Thread.sleep(25L);
				}
				profiler.popSection();

				profiler.pushSection("tests1s2");
				{
					Thread.sleep(5L);
				}
				profiler.popSection();

				profiler.pushSection("tests1s3");
				{
					profiler.pushSection("tests1s3s1");
					{
						Thread.sleep(5L);
					}
					profiler.popSection();
				}
				profiler.popSection();
			}
			profiler.popSection();
			profiler.pushSection("tests2");
			{
				Thread.sleep(5L);
			}
			profiler.popSection();

			profiler.stop();
			profiler.writeProfiling();

			Path profiling = root.resolve("profiling");
			profiler.writeToDisk(profiling);

			try (Profiler p2 = Profiler.readFromDisk(profiling)) {
				assertEquals(profiler.getMainSection(), p2.getMainSection());
			}
		}
	}

	@Test
	public void structTest() throws InterruptedException {
		try (Profiler profiler = new Profiler("test")) {
			profiler.setDisabled(false);
			profiler.pushSection("tests1");
			{
				profiler.pushSection("tests1s1");
				{
					Thread.sleep(25L);
				}
				profiler.popSection();

				profiler.pushSection("tests1s2");
				{
					Thread.sleep(5L);
				}
				profiler.popSection();

				profiler.pushSection("tests1s3");
				{
					profiler.pushSection("tests1s3s1");
					{
						Thread.sleep(5L);
					}
					profiler.popSection();
				}
				profiler.popSection();
			}
			profiler.popSection();
			profiler.pushSection("tests2");
			{
				Thread.sleep(5L);
			}
			profiler.popSection();

			profiler.stop();

			Profiler.Section test = profiler.getMainSection();
			assertEquals("test", test.getName());
			List<Profiler.Section> testSub = test.getSubSections();
			assertEquals(2, testSub.size());

			Profiler.Section tests1 = testSub.get(0);
			assertEquals("tests1", tests1.getName());
			List<Profiler.Section> tests1Sub = tests1.getSubSections();
			assertEquals(3, tests1Sub.size());

			Profiler.Section tests1s1 = tests1Sub.get(0);
			assertEquals("tests1s1", tests1s1.getName());
			List<Profiler.Section> tests1s1Sub = tests1s1.getSubSections();
			assertEquals(0, tests1s1Sub.size());

			Profiler.Section tests1s2 = tests1Sub.get(1);
			assertEquals("tests1s2", tests1s2.getName());
			List<Profiler.Section> tests1s2Sub = tests1s2.getSubSections();
			assertEquals(0, tests1s2Sub.size());

			Profiler.Section tests1s3 = tests1Sub.get(2);
			assertEquals("tests1s3", tests1s3.getName());
			List<Profiler.Section> tests1s3Sub = tests1s3.getSubSections();
			assertEquals(1, tests1s3Sub.size());

			Profiler.Section tests1s3s1 = tests1s3Sub.get(0);
			assertEquals("tests1s3s1", tests1s3s1.getName());
			assertEquals(0, tests1s3s1.getSubSections().size());

			Profiler.Section tests2 = testSub.get(1);
			assertEquals("tests2", tests2.getName());
			assertEquals(0, tests2.getSubSections().size());
		}
	}

	@Test
	public void loadBack() {
		long id;
		try (Profiler prof = new Profiler("test")) {
			prof.setDisabled(false);
			id = prof.getId();
			try (Profiler p2 = Profiler.getProfilerById(id)) {
				assertNotNull(p2);
				assertEquals(prof, p2);
			}
		}
		assertNull(Profiler.getProfilerById(id));
	}

	@Test
	public void loadBackOpt() {
		HDTOptions opt = HDTOptions.of();
		long id;
		try (Profiler prof = Profiler.createOrLoadSubSection("test", opt, true)) {
			id = prof.getId();
			Profiler p2 = Profiler.getProfilerById(id);
			assertNotNull(p2);
			assertEquals(prof, p2);

			try (Profiler p3 = Profiler.createOrLoadSubSection("test2", opt, true)) {
				assertNotNull(p3);
				assertEquals(prof, p3);
			}
		}
		assertNull(Profiler.getProfilerById(id));
	}

	@Test(expected = IllegalArgumentException.class)
	public void popTest() {
		try (Profiler p = new Profiler("")) {
			p.setDisabled(false);
			p.popSection();
		}
	}
}
