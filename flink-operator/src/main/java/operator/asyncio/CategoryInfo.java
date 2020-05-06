package operator.asyncio;

public class CategoryInfo {
    private Long subCategoryId;
    private Long parentCategoryId;

    public CategoryInfo(Long sub_category_id, Long parentCategoryId) {
        this.subCategoryId = sub_category_id;
        this.parentCategoryId = parentCategoryId;
    }

    public Long getSubCategoryId() {
        return subCategoryId;
    }

    public void setSubCategoryId(Long subCategoryId) {
        this.subCategoryId = subCategoryId;
    }

    public Long getParentCategoryId() {
        return parentCategoryId;
    }

    public void setParentCategoryId(Long parentCategoryId) {
        this.parentCategoryId = parentCategoryId;
    }

    @Override
    public String toString() {
        return "CategoryInfo{" +
                "subCategoryId='" + subCategoryId + '\'' +
                ", parentCategoryId='" + parentCategoryId + '\'' +
                '}';
    }
}
