# 6.824lab

## 引言

分布式系统是现在计算机软件系统中不可避免的一种架构，了解分布式系统对于构建任何大型分布式应用，对于理解分布式程序的运行，对于优化分布式程序的运行环境都有一定的帮助。

mit6.824 这门课程可以说是明星课程了，主讲老师是Robert Morris，这个老师是个传奇人物。能够听这样的传奇人物叨叨十几个小时，本身就是一种享受，更何况Robert教授能够一种理论联系实际的方式，将主流的分布式系统软件讲的浅显易懂。

这门课程总共有20节课，4个实验，实验都是基于golang完成，课程配套了实验相关的测试用例，动手完成实验可以加深对于相关知识的理解。所有课程内容可以在课程网站（[6.824: Distributed Systems - Spring 2022](https://pdos.csail.mit.edu/6.824/index.html)）找到。

本项目使用go完成了四个实验，通过了 6.824 lab 的测试。

## 介绍

master分支：从课程网站（[6.824: Distributed Systems - Spring 2022](https://pdos.csail.mit.edu/6.824/index.html)）代码库中克隆的原始模板代码-`git clone git://g.csail.mit.edu/6.824-golabs-2022 6.824`

lab1-mapreduce分支：实验一，mapreduce的实现

lab2-raft分支：实验二，Raft算法的实现

lab3-kvraft分支：实验三，在实验二的基础上实现一个可容错的内存KV数据库

lab4-shardkv分支：实验四，在实验三的基础上实现一个可容错的、分片的内存KV数据库

all-labs分支：四个实验整合在一起的最终版本代码

## 参考

[课程官网-6.824: Distributed Systems - Spring 2022](https://pdos.csail.mit.edu/6.824/index.html)

[mit-6.824实验说明](https://pdos.csail.mit.edu/6.824/labs/guidance.html)

[B站中文字幕视频](https://www.bilibili.com/video/BV1x7411M7Sf?spm_id_from=333.337.search-card.all.click)
