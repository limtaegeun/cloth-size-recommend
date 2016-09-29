import preprocess

def filter_category(rdd, categorys, sex):
    def filter_func(row):

        if row.category not in categorys:
            return False

        if row.gender not in sex:
            return False
        return True

    return rdd.filter(filter_func)
def map_get_error(rdd, user_rdd):

    def get_error(row):

        # --------------matched detail measure---------------
        """
        user              -   clothes
        shoulder          -   shoulderWidth * 2
        chest             -   chestRound / chestWidth * 2
        waist / pelvis    -   waistRound / chestWidth * 2 / pelvisRound
        hip               -   hipRound /  hipWidth * 2
        thigh             -   thighRound / thighWidth * 2
        arm               -   sleeveLength
        leg               -   legLength
        calf              -   calfRound / calfWidth * 2
        """
        # ---------------------------------------------------

        row_list = []

        for user in user_rdd:

            user_dic = user.asDict()

            if user_dic[u'sex'] == u'm':

                fit_size = unicode(user_dic['top_size'])  # fit_size = user['fit_size']

                try:
                    size_index = row.size.index(fit_size)
                except ValueError:
                    pass
                else:
                    shoulder = (row.shoulder[size_index] * 2 - user_dic['shoulder']) if row.shoulder[size_index] != 0 else 0
                    chest = (row.chest[size_index] - user_dic['chest']) if row.chest[size_index] != 0 else 0
                    waist = (row.waist[size_index] - user_dic['waist']) if row.waist[size_index] != 0 else 0
                    hip = (row.hip[size_index] - user_dic['hip']) if row.hip[size_index] != 0 else 0
                    thigh = (row.thigh[size_index] - user_dic['thigh']) if row.thigh[size_index] != 0 else 0

                    pelvis = 0
                    if row.waist[size_index] != 0:
                        pelvis = (row.waist[size_index] - user_dic['pelvis'])
                    elif row.pelvis[size_index] != 0:
                        pelvis = (row.pelvis[size_index] - user_dic['pelvis'])
                    else:
                        pelvis = 0

                    # error_model_basic = Row('size', 'shoulder', 'chest', 'waist', 'pelvis', 'hip', 'thigh')
                    row_list.append(preprocess.error_model_basic(row.name,row.size[size_index], shoulder, chest, waist, pelvis, hip, thigh))


        return row_list

    return rdd.flatMap(get_error)

def get_data(rdd, user_rdd, categorys, sex):

    filtering_rdd = filter_category(rdd, categorys, sex)

    return map_get_error(filtering_rdd, user_rdd)