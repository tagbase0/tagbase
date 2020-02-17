package com.oppo.tagbase.meta.obj;

import java.sql.Date;
import java.util.Objects;

/**
 * Created by wujianchao on 2020/2/17.
 */
public class Dict {

    private long id;
    private String version = "1.0.0";
    private DictStatus status;
    private String location;
    private long length;
    private Date createDate;
    private DictType type;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public DictStatus getStatus() {
        return status;
    }

    public void setStatus(DictStatus status) {
        this.status = status;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public DictType getType() {
        return type;
    }

    public void setType(DictType type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Dict that = (Dict) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
