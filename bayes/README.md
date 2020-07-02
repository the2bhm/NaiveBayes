## 基于Hadoop平台设计MapReduce程序

### 实现功能：

1.3机器学习 算法：Naive Bayes



### 实验环境：

Hadoop 2.7.7，java 10，即云计算课堂实验2的实验环境。



### 项目简介

1. 实现了NaiveBayes算法

2. 使用的数据集为：`iris.data`。机器学习中比较经典的鸢尾花数据集。数据集一共5列150行，每一列分别代表鸢尾花的 花萼长度、花萼宽度、花瓣长度、花瓣宽度、鸢尾花的种类。

3. 解决的问题为通过这4个属性预测鸢尾花的品种。

   

### 关键函数介绍

* `splitvariables`函数，将数据集中的数据按‘,'分隔开存入HashSet中。

```java
public HashSet<Integer> splitvariables(String varString){
		HashSet<Integer> hs = new HashSet<Integer>();
		//构造解析varString的对象，并指定分隔符为','.
	    StringTokenizer tok = new StringTokenizer(varString,",");
	    while(tok.hasMoreElements())
	    	hs.add(Integer.parseInt(tok.nextToken()));
		return hs;
	}
```



* `configure`函数,根据输入定义分隔符，数据的列数，连续变量，离散变量，目标变量，这些输入值由数据集来决定。

  ```java
  	//传入数据
  	@Override
  	 public void configure(JobConf conf){
  		//分隔符
  		delimiter = conf.get("delimiter");
  		numColums = conf.getInt("numColumns", 0);
  		continousVariables = conf.get("continousVariables");
  		discreteVariables = conf.get("discreteVariables");
  	    targetVariable = conf.getInt("targetVariable",0);
  	    discreteVariablesIndex = new HashSet<Integer>();
  		continousVariablesIndex = new HashSet<Integer>();
  		//分隔传入变量的数据
  	    if(continousVariables!=null)
  	    continousVariablesIndex = splitvariables(continousVariables);
  	    if(discreteVariables!=null)
  	    discreteVariablesIndex = splitvariables(discreteVariables);
  	}
  ```



* `map`函数,处理数据集中的数据，将不同列的属性分别列出来，格式为 ‘属性编号'+'_'+'鸢尾花种类'。

  ```java
  public void map(LongWritable arg0, Text value,
  			OutputCollector<Text, DoubleWritable> output, Reporter arg3)
  			throws IOException {
  		Integer varIndex = 1; 
  		String record = value.toString();
  		//分隔数据集中的数据
  		String features[] = record.split(delimiter);
  	    for(int i = 0 ;i < numColums ; i++){//数据集的行数
  	    	if(varIndex!= targetVariable){
  	    		if(discreteVariablesIndex.contains(varIndex))
  	    		 output.collect(new Text(varIndex+"_"+features[i]+"_"+features[targetVariable-1]), new DoubleWritable(1.0));
  	    		if(continousVariablesIndex.contains(varIndex))
  	    		 output.collect(new Text(varIndex+"_"+features[targetVariable-1]), new DoubleWritable(Double.parseDouble(features[i])));
  	    	}
  	    	varIndex ++;
  	}
  	    output.collect(new Text(targetVariable+"_"+features[targetVariable-1]), new DoubleWritable(1.0));
  	    output.collect(new Text(targetVariable+""), new DoubleWritable(1.0));
  	}
  ```



* `reduce`函数,计算处理好的数据，计算两个结果均值和方差。

  ```java
  public void reduce(Text keyId, Iterator<DoubleWritable> values,
  			OutputCollector<NullWritable, Text> output, Reporter arg3) throws IOException {
  		String id = keyId.toString().split("_")[0];
  		if(continousVariablesIndex.contains(Integer.parseInt(id))){
  			double sumsqr=0,sum = 0,count=0,tmp;
  			double mean,var;
  			 while (values.hasNext())
  	          {
  	        	   tmp=values.next().get();
  	        	   sumsqr+=tmp*tmp;
  	               sum += tmp;
  	               count++;
  			  }
  			 //求数据集中4个属性的均值
  			 mean=sum/count;
  			 //求数据集对应属性的方差
  			 var=(sumsqr-((sum*sum)/count))/count;
  	         output.collect(NullWritable.get(), new Text(keyId+" "+mean+","+var));
  		}
  		if(discreteVariablesIndex.contains(Integer.parseInt(id))){
  			Double count = 0.0;
  			while (values.hasNext())
  	          count +=  values.next().get();
  			  output.collect(NullWritable.get(), new Text(keyId+" "+count.toString()));
  		}
  		if(targetVariable == Integer.parseInt(id)){
  			Double count = 0.0;
  			while (values.hasNext())
  	          count +=  values.next().get();
  			  output.collect(NullWritable.get(), new Text(keyId+" "+count.toString()));
  		}
  	}
  ```



* `calculateProbablity`函数,计算离散型的条件概率。

  ```java
  	//计算条件概率
  	double calculateProbablity(int featureID,String value,String label){
  	   String classCount,valueCount,totalCount;
  	   classCount = hm.get(targetVariable+"_"+label);
  	   if(classCount==null)
  		   return 1.0;
  	   valueCount = hm.get(featureID+"_"+value+"_"+label);
  	   if(valueCount==null)
  		   return 1.0;
  	   totalCount = hm.get(targetVariable+"");
  	   double classProbablity = (Double.parseDouble(classCount) / Double.parseDouble(totalCount));
  	   double valueProbablity = (Double.parseDouble(valueCount) / Double.parseDouble(classCount));
  	   return (classProbablity*valueProbablity);
  	}
  ```



* `calculateGaussian`函数，计算连续型的条件概率。

  ```java
  //计算正态分布的概率密度
  	double calculateGaussian(int featureID,String value,String label){
  		Double mean,variance,val;
  		val = Double.parseDouble(value);
  		String values = hm.get(featureID+"_"+label);
  		if(values!=null){
  	      StringTokenizer tokMeanVariance = new StringTokenizer(values,",");
  	      mean = Double.parseDouble(tokMeanVariance.nextToken());
  	      variance = Double.parseDouble(tokMeanVariance.nextToken());
  	      if(variance==0.0)
  	    	  return 1.0;
  	      double exponent,denaminator;
  	      denaminator = Math.sqrt(2*3.414)*variance;
  	      exponent = -1*(Math.pow((val-mean),2))/(2*Math.pow(variance, 2));
  	      return (1/denaminator)*Math.exp(exponent);
  		}
  		return 1.0;
  	}
  ```

  

