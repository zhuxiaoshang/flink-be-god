package operator.asyncio;

public class StoreInfo {
    private String strCd;
    private String strNm;

    public StoreInfo(String strCd, String strNm) {
        this.strCd = strCd;
        this.strNm = strNm;
    }

    public String getStrCd() {
        return strCd;
    }

    public void setStrCd(String strCd) {
        this.strCd = strCd;
    }

    public String getStrNm() {
        return strNm;
    }

    public void setStrNm(String strNm) {
        this.strNm = strNm;
    }

    @Override
    public String toString() {
        return "StoreInfo{" +
                "strCd='" + strCd + '\'' +
                ", strNm='" + strNm + '\'' +
                '}';
    }
}
