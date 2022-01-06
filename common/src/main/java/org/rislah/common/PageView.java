package org.rislah.common;

public class PageView {
    private String platform;
    private String userId;

    public PageView(String platform, String userId) {
        this.platform = platform;
        this.userId = userId;
    }

    public PageView() {
    }

    @Override
    public String toString() {
        return "PageView{" +
                "platform='" + platform + '\'' +
                ", userId='" + userId + '\'' +
                '}';
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getUserId() {
        return userId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PageView pageView = (PageView) o;

        if (!platform.equals(pageView.platform)) return false;
        return userId.equals(pageView.userId);
    }

    @Override
    public int hashCode() {
        int result = platform.hashCode();
        result = 31 * result + userId.hashCode();
        return result;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
