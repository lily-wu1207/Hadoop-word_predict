# Hadoop-word_predict
using MapReduce structure to predict the next input word 
## 文件说明
### GUI   
利用mapreduce计算的模型，通过简单的stupid-backoff平滑取概率最大的前20个可能出现的字进行显示，利用vue进行前后端交互，实现实时预测下一个输入汉字的功能  
前端界面：  
![image](https://github.com/lily-wu1207/Hadoop-word_predict/assets/105954052/5666c218-c682-4d09-bf98-6f349bb9375c)
后端界面：  
![image](https://github.com/lily-wu1207/Hadoop-word_predict/assets/105954052/9d3141a1-6b09-43cb-a160-0fb1b452fd37)
### test
test和test2是实验用到的语料库，分别为《红楼梦》全文以及《三体》全册   
### Java
用java语言通过hadoop实现对输入语料的N-gram计数，mapreduce的输出为 < k-gram, {next_word, count} > 的形式。由于用到的语料库相对比较小，因此k取<=4，对于更大的语料库可以考虑采用 < <word, next_word>, count > 的形式避免内存瓶颈。  
整体框架：  
![image](https://github.com/lily-wu1207/Hadoop-word_predict/assets/105954052/fc92bdfd-1097-4beb-a992-952669b88d98)
reduce中通过统计频率+平滑计算概率：  
![image](https://github.com/lily-wu1207/Hadoop-word_predict/assets/105954052/e9ac34c8-1b82-45c7-b0b5-f4f205e87425)
输出格式：  
![image](https://github.com/lily-wu1207/Hadoop-word_predict/assets/105954052/1770214b-a6cc-470e-8322-415ea6542cc7)

