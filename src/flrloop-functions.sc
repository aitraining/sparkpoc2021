val name = "venu"
val age = 99
//if-else or match .... one variable,
//process multi values ...for loop

//array means collection of same datatype elements

val nums = Array(22,33,22,11,55,44,33,99,39,-44)

//for loop
val test = for(x<- nums) println(x*x)

def table(x:Int) = {
   //val num = 1 to x toArray;
   val num2 = 1 to 10 toArray;
   val res = for(b<-num2) println(s"$x *$b = ${x*b}")
   res
}

def table1(x:Int) = {
   val num = 1 to x toArray;
   val num2 = 1 to 10 toArray;
   val res = for(a<-num;b<-num2) println(s"$a *$b = ${a*b}")
   res}