package Demo;

public class RangeHandler 
{
private int min,max;

public RangeHandler(int min, int max) 
{
	this.min = min;
	this.max = max;
}
public RangeHandler() 
{
	this.min =0;
	this.max = 0;
}

public int getMin() {
	return min;
}
public void setMin(int min) {
	this.min = min;
}

public int getMax() {
	return max;
}
public void setMax(int max) {
	this.max = max;
}

@Override
public boolean equals(Object o) 
{
	RangeHandler ob=(RangeHandler)o;
	if((this.min==ob.min)&&(this.max==ob.max))
		return true;
	else
		return false;
}


}
