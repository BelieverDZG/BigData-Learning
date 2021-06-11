package dzg.java.prost.trieIndex;

/**
 * @author BelieverDzg
 * @date 2021/3/31 12:43
 */
public class D {

    int res = 0;//最终的糖果数

    public void buyCandy(int money) {
        if (money < 5) return;
        int temp = money / 5;//现在的钱可以买的糖果数
        int newMoney = temp * 3 + money % 5; //糖果纸换的钱 + 买糖果剩的钱
        System.out.println(temp + " , " + newMoney);
        res += temp;
        buyCandy(newMoney);
    }

    public static void main(String[] args) {
        D d = new D();
        d.buyCandy(100);
        System.out.println(d.res);
    }
}
