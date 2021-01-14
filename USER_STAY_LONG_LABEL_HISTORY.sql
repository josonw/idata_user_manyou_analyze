
insert overwrite table bdp_dw.USER_STAY_LONG_LABEL_HISTORY partition (dt='202006')
            select * from bdp_dw.user_stay_long_label