package com.atguigu.online.education.common.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsLearnchapterPlaywindowBean {

    //窗口起始时间
    String stt;
    //窗口结束时间
    String edt;
    //视频id
    String videoId;
    //章节id
    String chapterId;
    //章节名称
    String chapterName;
    //用户id
    String userId;
    //播放次数
    Long playCount;
    // 播放总时长
    Long playDuration;
    //观看人数
    Long playUserCount;
    //时间戳
    Long ts;


}
