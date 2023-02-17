package com.the_qa_company.qendpoint.core.hdtCat;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdtCat.utils.Utility;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class HdtCatLiteralsTest implements ProgressListener {

    public void help(String file1, String file2, String concat){
        if (SystemUtils.IS_OS_UNIX) {
            try {
                ClassLoader classLoader = getClass().getClassLoader();
                HDT hdt1 = null;
                HDT hdt2 = null;
                String hdt1_location = file1.replace(".nt", ".hdt");
                String hdt2_location = file2.replace(".nt", ".hdt");
                HDTSpecification spec = new HDTSpecification();
                spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
                hdt1 = HDTManager.generateHDT(new File(file1).getAbsolutePath(), "uri", RDFNotation.NTRIPLES, spec, this);
                hdt1.saveToHDT(hdt1_location, null);
                hdt2 = HDTManager.generateHDT(new File(file2).getAbsolutePath(), "uri", RDFNotation.NTRIPLES, spec, this);
                hdt2.saveToHDT(hdt2_location, null);
                HDT hdtCatOld = HDTManager.generateHDT(new File(concat).getAbsolutePath(), "uri", RDFNotation.NTRIPLES, spec, this);

                File file = new File(file1);
                File theDir = new File(file.getAbsolutePath() + "_tmp");
                theDir.mkdirs();
                HDT hdtCatNew = HDTManager.catHDT(theDir.getAbsolutePath(), hdt1_location, hdt2_location, spec, null);
               hdtCatNew.saveToHDT(file.getAbsolutePath()+"_cat.hdt",null);
               HDT hdt = HDTManager.mapIndexedHDT(file.getAbsolutePath()+"_cat.hdt",spec);
                //HDTCat hdtCatNew = new HDTCat(new HDTSpecification(),hdt1,hdt2,this);

//                System.out.println("HDT1 ----------------------------------");
//                Utility.printCustomDictionary(hdt1.getDictionary());
//                System.out.println("HDT2 ----------------------------------");
//                Utility.printCustomDictionary(hdt2.getDictionary());
                System.out.println("original ----------------------------------");
                Utility.printCustomDictionary(hdtCatOld.getDictionary());

                System.out.println("NEW ----------------------------------");
                Utility.printCustomDictionary(hdtCatNew.getDictionary());

                Utility.compareCustomDictionary(hdtCatOld.getDictionary(), hdtCatNew.getDictionary());
//                Utility.printTriples(hdtCatOld);
                Utility.compareTriples(hdtCatOld,hdtCatNew);

                try {
                    IteratorTripleString it = hdtCatOld.search("", "", "");
                    while (it.hasNext()) {
                        System.out.println(it.next().toString());
                    }

                } catch (NotFoundException e) {
                    e.printStackTrace();
                }
                System.out.println("------------------------------------");
                try {
                    IteratorTripleString it = hdtCatNew.search("", "", "");
                    while (it.hasNext()) {
                        System.out.println(it.next().toString());
                    }

                } catch (NotFoundException e) {
                    e.printStackTrace();
                }
                theDir.delete();
            } catch (ParserException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
//    @Test
//    public void misc(){
//        String file1 = "/Users/alyhdr/Desktop/qa-company/data/admin/eu/hdt_index/new_index_diff.hdt";
//        String file2 = "/Users/alyhdr/Desktop/qa-company/data/admin/eu/hdt_index/new_index_v2.hdt";
//
//        try {
//            HDTSpecification spec = new HDTSpecification();
//            spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
//            HDT hdt1 = HDTManager.mapHDT(file1);
//            HDT hdt2 = HDTManager.mapHDT(file2);
//            System.out.println(hdt1.getDictionary().stringToId("https://linkedopendata.eu/entity/Q3048056",TripleComponentRole.OBJECT));
//            IteratorTripleID search1 = hdt1.getTriples().search(new TripleID(0, 0, 12645790));
//            if(search1.hasNext()){
//                System.out.println(search1.next());
//            }
//            IteratorTripleString search = hdt1.search("", "", "https://linkedopendata.eu/entity/Q3048056");
//            if(search.hasNext()){
//                System.out.println(search.next());
//            }
//
//            Iterator<? extends CharSequence> no_datatype1 = hdt1.getDictionary().getAllObjects().get(LiteralsUtils.NO_DATATYPE).getSortedEntries();
//            Iterator<? extends CharSequence> no_datatype2 = hdt2.getDictionary().getAllObjects().get(LiteralsUtils.NO_DATATYPE).getSortedEntries();
//            while (no_datatype1.hasNext()){
//                CharSequence next1 = no_datatype1.next();
//                CharSequence next2 = no_datatype2.next();
//                System.out.println(next1+" "+next2);
//            }
////            for (Map.Entry<String, DictionarySection> section : hdt.getDictionary().getAllObjects().entrySet()) {
////                System.out.println("Checking section: "+section.getKey());
////                Iterator<? extends CharSequence> sortedEntries = section.getValue().getSortedEntries();
////                HashSet<CharSequence> set = new HashSet<>();
////                while (sortedEntries.hasNext()) {
////                    CharSequence next = sortedEntries.next();
////                    if (set.contains(next)) {
////                        System.out.println("Found duplicate: " + next);
////                    } else {
////                        set.add(next);
////                    }
////                }
////            }
//        } catch (IOException | NotFoundException e) {
//            e.printStackTrace();
//        }
//
//    }
    @Test
    public void cat0() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("empty1.nt").getFile();
        String file2 = classLoader.getResource("empty2.nt").getFile();
        String concat = classLoader.getResource("empty1+2.nt").getFile();
        help(file1,file2,concat);
    }
    @Test
public void cat1() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example1.nt").getFile();
        String file2 = classLoader.getResource("example2.nt").getFile();
        String concat = classLoader.getResource("example1+2.nt").getFile();
        help(file1,file2,concat);
    }

    @Test
    public void cat2() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example2.nt").getFile();
        String file2 = classLoader.getResource("example3.nt").getFile();
        String concat = classLoader.getResource("example2+3.nt").getFile();
        help(file1,file2,concat);
    }

    @Test
    public void cat3() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example4.nt").getFile();
        String file2 = classLoader.getResource("example5.nt").getFile();
        String concat = classLoader.getResource("example4+5.nt").getFile();
        help(file1,file2,concat);
    }

    @Test
    public void cat4() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example6.nt").getFile();
        String file2 = classLoader.getResource("example7.nt").getFile();
        String concat = classLoader.getResource("example6+7.nt").getFile();
        help(file1,file2,concat);
    }

    @Test
    public void cat5() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example8.nt").getFile();
        String file2 = classLoader.getResource("example9.nt").getFile();
        String concat = classLoader.getResource("example8+9.nt").getFile();
        help(file1,file2,concat);
    }

    @Test
    public void cat6() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example10.nt").getFile();
        String file2 = classLoader.getResource("example11.nt").getFile();
        String concat = classLoader.getResource("example10+11.nt").getFile();
        help(file1,file2,concat);
    }

    @Test
    public void cat7() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example12.nt").getFile();
        String file2 = classLoader.getResource("example13.nt").getFile();
        String concat = classLoader.getResource("example12+13.nt").getFile();
        help(file1,file2,concat);
    }

    @Test
    public void cat9() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example14.nt").getFile();
        String file2 = classLoader.getResource("example15.nt").getFile();
        String concat = classLoader.getResource("example14+15.nt").getFile();
        help(file1,file2,concat);
    }

    @Test
    public void cat10() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example16.nt").getFile();
        String file2 = classLoader.getResource("example17.nt").getFile();
        String concat = classLoader.getResource("example16+17.nt").getFile();
        help(file1,file2,concat);
    }

    @Test
    public void cat11() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example1.nt").getFile();
        String file2 = classLoader.getResource("example1.nt").getFile();
        String concat = classLoader.getResource("example1.nt").getFile();
        help(file1,file2,concat);
    }
    @Test
    public void cat12() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example18.nt").getFile();
        String file2 = classLoader.getResource("example19.nt").getFile();
        String concat = classLoader.getResource("example18+19.nt").getFile();
        help(file1,file2,concat);
    }
    @Test
    public void cat13() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example20.nt").getFile();
        String file2 = classLoader.getResource("example21.nt").getFile();
        String concat = classLoader.getResource("example20+21.nt").getFile();
        help(file1,file2,concat);
    }
    @Test
    public void cat14() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example22.nt").getFile();
        String file2 = classLoader.getResource("example23.nt").getFile();
        String concat = classLoader.getResource("example22+23.nt").getFile();
        help(file1,file2,concat);
    }
    @Test
    public void cat15() {
        ClassLoader classLoader = getClass().getClassLoader();
        String file1 = classLoader.getResource("example24.nt").getFile();
        String file2 = classLoader.getResource("example25.nt").getFile();
        String concat = classLoader.getResource("example24+25.nt").getFile();
        help(file1,file2,concat);
    }
    @Override
    public void notifyProgress(float level, String message) {

    }

}
