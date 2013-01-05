#!/bin/sh

#
# METAQ数据迁移工具，用来将SERVER本地数据迁移到其他机器
#
sh $(dirname $0)/run-class.sh com.taobao.metamorphosis.tools.dataTransfer.Transfer $@
