package com.oppo.tagbase.common.util;

import com.oppo.tagbase.common.ResourceResponse;
import com.oppo.tagbase.common.TagbaseException;

import javax.ws.rs.core.Response;

/**
 * Created by wujianchao on 2020/2/24.
 */
public class ResourceUtil {

    public static Response ok(Object o) {
        return Response.ok(ResourceResponse.ok(o)).build();
    }

    public static Response error(TagbaseException error) {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(ResourceResponse.error(error))
                .build();
    }

}
