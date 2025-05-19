
alter session set current_schema = SYSTEM;

-- 创建表空间
create tablespace ZYSERVICESPACE
datafile 'F:\soft\oracle_workspace'
size 1024m
autoextend on
next 100m
maxsize unlimited;
-- 创建用户，关联表空间 密码：zydev2021
create user ZYDEVER identified by "zydev2021"  default tablespace ZYSERVICESPACE profile default account unlock;

-- 为用户授权
grant connect, resource to ZYDEVER;
grant unlimited tablespace to ZYDEVER;


create table ZYDEVER.stuinfo     --用户名.表名
(
  stuid      varchar2(11) not null,--学号：①
  stuname    varchar2(50) not null,--学生姓名
  sex        char(1) not null,--性别
  age        number(2) not null,--年龄
  classno    varchar2(7) not null,--班号：
  stuaddress varchar2(100) default '地址未录入',--地址 ②
  grade      char(4) not null,--年级
  enroldate  date,--入学时间
  idnumber   varchar2(18) default '身份证未采集' not null--身份证
)
tablespace ZYSERVICESPACE --③处：表示表stuinfo存储的表空间是users，storage表示存储参数：
                            -- 区段(extent)一次扩展64k，最小区段数为1，最大的区段数不限制。
  storage
  (
    initial 64K
    minextents 1
    maxextents unlimited
  );

-- Add comments to the table
comment on table ZYDEVER.stuinfo --表名备注
  is '学生信息表';
-- Add comments to the columns
comment on column ZYDEVER.stuinfo.stuid -- 字段备注
  is '学号';


insert into ZYDEVER.stuinfo values (1,'小明','1',11,'2020','河北省','3',TO_DATE('2025-05-05','YYYY-MM-DD'),'是');
insert into ZYDEVER.stuinfo values (2,'小明2','2',11,'2020','河北省','3',TO_DATE('2025-05-05','YYYY-MM-DD'),'是');


SELECT *
FROM ZYDEVER.stuinfo
PIVOT (
  SUM(AGE)   -- 聚合函数
  FOR STUNAME IN ('小明', '小明2')  -- 要转换的列及取值
);


 -- 主键(PRIMARY KEY)约束
 ALTER TABLE ZYDEVER.stuinfo ADD CONSTRAINT pk_stuinfo_stuid PRIMARY KEY(stuid);
 -- 唯一（UNIQUE）约束
 ALTER TABLE ZYDEVER.STUINFO ADD CONSTRAINT uk_stuinfo_stuname UNIQUE (stuname);
 -- 条件（CHECK）约束
     --给字段年龄age添加约束，学生的年龄只能0-50岁之内的
 alter table ZYDEVER.STUINFO add constraint ch_stuinfo_age check(age>0 and age<=50);
     -- 限定sex的值
 alter table ZYDEVER.STUINFO add constraint ch_stuinfo_sex check(sex='1' or sex='0');
     -- 限定年级的范围
 alter table ZYDEVER.STUINFO add constraint ch_stuinfo_grade check (grade>='1900' and grade<='2999');





-----------test 行转列
CREATE TABLE ZYDEVER.SALES (
    PRODUCT    VARCHAR2(50) NOT NULL,
    QUARTER    VARCHAR2(10) NOT NULL,
    AMOUNT     NUMBER(10, 2) NOT NULL
    -- 可选：添加主键约束
--     CONSTRAINT pk_sales PRIMARY KEY (PRODUCT, QUARTER)
);

-- 插入示例数据
INSERT INTO ZYDEVER.SALES (PRODUCT, QUARTER, AMOUNT) VALUES ('Apple', 'Q1', 1000);
INSERT INTO ZYDEVER.SALES (PRODUCT, QUARTER, AMOUNT) VALUES ('Apple', 'Q2', 1500);
INSERT INTO ZYDEVER.SALES (PRODUCT, QUARTER, AMOUNT) VALUES ('Banana', 'Q1', 800);
INSERT INTO ZYDEVER.SALES (PRODUCT, QUARTER, AMOUNT) VALUES ('Banana', 'Q2', 900);

-- 提交事务
COMMIT;
select * from ZYDEVER.SALES;
SELECT *
FROM ZYDEVER.SALES
PIVOT (
  SUM(AMOUNT)  -- 聚合函数
  FOR QUARTER IN ('Q1', 'Q2')  -- 要转换的列及取值
);

--------test 列转行
CREATE TABLE ZYDEVER.EMP_SALARY (
    EMP_ID NUMBER(10) PRIMARY KEY,
    JAN    NUMBER(10, 2),
    FEB    NUMBER(10, 2),
    MAR    NUMBER(10, 2)
);

-- 插入示例数据
INSERT INTO ZYDEVER.EMP_SALARY (EMP_ID, JAN, FEB, MAR) VALUES (101, 5000, 6000, 5500);
INSERT INTO ZYDEVER.EMP_SALARY (EMP_ID, JAN, FEB, MAR) VALUES (102, 4500, 4800, 5200);

-- 提交事务
COMMIT;


SELECT *
FROM ZYDEVER.EMP_SALARY
UNPIVOT (
    SALARY
    FOR MONTH IN (JAN, FEB, MAR)
        );
