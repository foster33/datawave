package datawave.query.rewrite.jexl.visitors;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PushdownLargeFieldedListsVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import net.bytebuddy.implementation.bytecode.Throw;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.constraints.AssertTrue;

public class PushdownLargeFieldedListsVisitorTest {
    protected ShardQueryConfiguration conf = null;
    
    @Before
    public void setupConfiguration() {
        conf = new ShardQueryConfiguration();
        conf.setMaxOrExpansionThreshold(3);
        conf.setMaxOrExpansionFstThreshold(100);
        conf.setIndexedFields(Sets.newHashSet("FOO", "BAR", "FOOBAR"));
    }
    
    @Test
    public void testSimpleExpression() throws Throwable {
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(conf,
                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery("FOO == 'BAR'")), null, null));
        Assert.assertEquals("FOO == 'BAR'", rewritten);
    }
    
    @Test
    public void testMultipleExpression() throws Throwable {
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(conf,
                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery("FOO == 'BAR' || FOO == 'FOO' || BAR == 'FOO'")), null, null));
        String expected = "BAR == 'FOO' || FOO == 'BAR' || FOO == 'FOO'";
        Assert.assertEquals("EXPECTED: " + expected + "\nACTUAL: " + rewritten, expected, rewritten);
    }
    
    @Test
    public void testPushdown() throws Throwable {
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(conf,
                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery("FOO == 'BAR' || FOO == 'FOO' || FOO == 'FOOBAR'")), null, null));
        String id = rewritten.substring(rewritten.indexOf("id = '") + 6, rewritten.indexOf("') && (field"));
        Assert.assertEquals("((_List_ = true) && ((id = '" + id + "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}')))",
                        rewritten);
    }
    
    @Test
    public void testEscapedValues() throws Throwable {
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(conf,
                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery("FOO == 'BAR' || FOO == 'FOO' || FOO == 'FOO,BAR'")), null, null));
        String id = rewritten.substring(rewritten.indexOf("id = '") + 6, rewritten.indexOf("') && (field"));
        Assert.assertEquals("((_List_ = true) && ((id = '" + id + "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOO,BAR\"]}')))",
                        rewritten);
    }
    
    @Test
    public void testPushdownPartial() throws Throwable {
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(conf, TreeFlatteningRebuildingVisitor
                        .flatten(JexlASTHelper.parseJexlQuery("FOO == 'BAR' || BAR == 'BAR' || FOO == 'FOO' || BAR == 'FOO' || FOO == 'FOOBAR'")), null, null));
        String id = rewritten.substring(rewritten.indexOf("id = '") + 6, rewritten.indexOf("') && (field"));
        String expected = "BAR == 'BAR' || BAR == 'FOO' || ((_List_ = true) && ((id = '" + id
                        + "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}')))";
        Assert.assertEquals("EXPECTED: " + expected + "\nACTUAL: " + rewritten, expected, rewritten);
    }
    
    @Test
    public void testPushdownMultiple() throws Throwable {
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(conf, TreeFlatteningRebuildingVisitor
                        .flatten(JexlASTHelper
                                        .parseJexlQuery("FOO == 'BAR' || BAR == 'BAR' || FOO == 'FOO' || BAR == 'FOO' || FOO == 'FOOBAR' || BAR == 'FOOBAR'")),
                        null, null));
        String id1 = rewritten.substring(rewritten.indexOf("id = '") + 6, rewritten.indexOf("') && (field"));
        String id2 = rewritten.substring(rewritten.lastIndexOf("id = '") + 6, rewritten.lastIndexOf("') && (field"));
        Assert.assertEquals("((_List_ = true) && ((id = '" + id1
                        + "') && (field = 'BAR') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}'))) || ((_List_ = true) && ((id = '" + id2
                        + "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}')))", rewritten);
    }
    
    @Test
    public void testPushdownIgnoreAnyfield() throws Throwable {
        String rewritten = JexlStringBuildingVisitor
                        .buildQuery(PushdownLargeFieldedListsVisitor.pushdown(
                                        conf,
                                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper
                                                        .parseJexlQuery("FOO == 'BAR' || _ANYFIELD_ == 'BAR' || FOO == 'FOO' || _ANYFIELD_ == 'FOO' || FOO == 'FOOBAR' || _ANYFIELD_ == 'FOOBAR'")),
                                        null, null));
        String id = rewritten.substring(rewritten.indexOf("id = '") + 6, rewritten.indexOf("') && (field"));
        Assert.assertEquals(
                        "((_List_ = true) && ((id = '"
                                        + id
                                        + "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}'))) || _ANYFIELD_ == 'BAR' || _ANYFIELD_ == 'FOO' || _ANYFIELD_ == 'FOOBAR'",
                        rewritten);
    }
    
    @Test
    public void testPushdownIgnoreOtherNodes() throws Throwable {
        conf.setMaxOrExpansionThreshold(1);
        String rewritten = JexlStringBuildingVisitor
                        .buildQuery(PushdownLargeFieldedListsVisitor.pushdown(
                                        conf,
                                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper
                                                        .parseJexlQuery("f:includeRegex(FOO, 'blabla') || FOO == 'BAR' || _ANYFIELD_ == 'BAR' || FOO == 'FOO' || _ANYFIELD_ == 'FOO' || FOO == 'FOOBAR' || _ANYFIELD_ == 'FOOBAR'")),
                                        null, null));
        String id = rewritten.substring(rewritten.indexOf("id = '") + 6, rewritten.indexOf("') && (field"));
        Assert.assertEquals(
                        "f:includeRegex(FOO, 'blabla') || ((_List_ = true) && ((id = '"
                                        + id
                                        + "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}'))) || _ANYFIELD_ == 'BAR' || _ANYFIELD_ == 'FOO' || _ANYFIELD_ == 'FOOBAR'",
                        rewritten);
    }
}
