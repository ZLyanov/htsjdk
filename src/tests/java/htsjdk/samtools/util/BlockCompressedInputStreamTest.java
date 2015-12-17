/*
 * The MIT License
 *
 * Copyright (c) 2009 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package htsjdk.samtools.util;

import java.io.File;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

public class BlockCompressedInputStreamTest {
    private static final File TEST_DATA_DIR = new File("testdata/htsjdk/samtools");

    @Test(dataProvider = "dataProvider")
    public void testBasic(final String bam) throws Exception {
        Assert.assertEquals(countRecords(bam, false), countRecords(bam, true));

    }

    private int countRecords(final String bam, final boolean async) {
        int numRecords = 0;
        final SamReader reader = SamReaderFactory.makeDefault().async(async).open(new File(TEST_DATA_DIR, bam));
        final SAMRecordIterator it = reader.iterator();
        while (it.hasNext()) {
            it.next();
            ++numRecords;
        }
        CloserUtil.close(reader);
        return numRecords;
    }

    @DataProvider(name = "dataProvider")
    public Object[][] bams() {
        return new Object[][]{
                {"compressed.bam"},
                {"BAMFileIndexTest/index_test.bam"}
        };
    }


}
