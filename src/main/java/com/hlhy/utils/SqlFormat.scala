package com.hlhy.utils

import java.io.{BufferedWriter, FileInputStream, FileWriter}

object SqlFormat {
  def main(args: Array[String]): Unit = {
        file("D:\\data\\IDEA\\flinksql\\src\\main\\resources\\test.sql")
  }


  def read(fileName: String): String = {
    val lines =scala.io.Source.fromInputStream(new FileInputStream(fileName),"utf-8").getLines
    val sb = new StringBuilder()
    lines.foreach(sb.append(_).append("\n"))
    return sb.toString
  }

  def replaceHql(hql:String, key:String):String = {
    return hql.replaceAll("\\b" + key + "\\b",key.toUpperCase)
  }

  def file(fileName: String): Unit = {
    var hql=read(fileName)
    val keyWord =read("D:\\data\\IDEA\\flinksql\\src\\main\\resources\\sql_keyword.txt")
    val keyArr=keyWord.split(",")
    for(w <- keyArr) {
      hql =replaceHql(hql,w)
    }
    //    println(hql)

    val bufferWriter =new BufferedWriter(new FileWriter(fileName))
    bufferWriter.write(hql)
    bufferWriter.close
  }

}

