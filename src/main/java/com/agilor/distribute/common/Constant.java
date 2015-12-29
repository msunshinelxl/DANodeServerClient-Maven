package com.agilor.distribute.common;

public class Constant {
	public final static String zkNodeClientFinalListName="finalList";
	public final static String zkNodeClientTmpListName="tmpList";
	public final static String zRootNode="/agilorRootPath";
	public final static int agilorNodeThriftPort=9090;
    public final static int agilorServerPort=10001;
	public final static int agilorNodeServerTimeout=20000;
	public final static int agilorNodeThriftLongTimeout=200000;
	public final static int zkTimeNormal=10000;
	public final static int zkTimeShort=3000;
	public final static int zkTimeLong=100000;
	public final static int TYPEFLOAT=1;
    public final static int TYPEINT=2;
    public final static int TYPESTR=3;
    public final static int TYPEBOOL=4;
	public final static String deviceNamePre="%#_DeviceState_";

	public final static int ERROR_FROM_AGILOR=5;
	public final static int ERROR_DISTRIBUTION_INFO=-3;
	public final static int ERROR_AGILORINI_FAIL=-2;
	public final static int ERROR_DEFAULT=-1;
	public final static int SUCESS=0;

}
