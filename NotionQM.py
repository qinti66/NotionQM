import os
from notion_client import Client, APIResponseError
from dotenv import load_dotenv
from typing import List, Dict, Any
import logging
import datetime
from functools import lru_cache

# 环境变量加载
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NotionTaskManager:
    """Notion任务管理增强版"""

    def __init__(self):
        self.config = self._load_config()
        self.notion = Client(auth=self.config["NOTION_API_KEY"])

    def _load_config(self) -> dict:
        """加载并验证配置"""
        required_fields = {
            "NOTION_API_KEY": "必须配置API密钥",
            "NOTION_DATABASE_ID": "必须配置数据库ID",
            "FREQUENCY_STATUS": "必须配置频率状态",
            "STATUS_COLUMNS": "必须配置状态列",
            "TARGET_STATUS": "必须配置目标状态"
        }

        config = {
            "FREQUENCY_STATUS": self._parse_list(os.getenv("FREQUENCY_STATUS")),
            "STATUS_COLUMNS": self._parse_list(os.getenv("STATUS_COLUMNS")),
            "TARGET_STATUS": os.getenv("TARGET_STATUS"),
            "NOTION_API_KEY": os.getenv("NOTION_API_KEY"),
            "NOTION_DATABASE_ID": os.getenv("NOTION_DATABASE_ID"),
            "TIME_COLUMN": os.getenv("TIME_COLUMN"),
            "FREQUENCY_NAME": os.getenv("FREQUENCY_NAME")
        }

        missing = [k for k, v in required_fields.items() if not config.get(k)]
        if missing:
            raise ValueError("\n".join([required_fields[k] for k in missing]))

        return config

    @lru_cache(maxsize=1)
    def _get_db_schema(self) -> dict:
        """获取数据库架构（带缓存）"""
        try:
            return self.notion.databases.retrieve(self.config["NOTION_DATABASE_ID"])
        except APIResponseError as e:
            logger.error(f"架构获取失败: {e.body}")
            raise

    def _build_frequency_filter(self) -> dict:
        """构建频率字段的动态过滤器"""
        schema = self._get_db_schema()
        prop_info = schema["properties"][self.config["FREQUENCY_NAME"]]

        # 修正键名统一问题
        filter_type_map = {
            "rich_text": {
                "type": "rich_text",
                "operator": "contains",
                "value_mapper": lambda x: x
            },
            "select": {
                "type": "select",
                "operator": "equals",
                "value_mapper": lambda x: x
            },
            "multi_select": {  
                "type": "multi_select",  
                "operator": "contains",
                "value_mapper": lambda x: x
            }
        }

        prop_type = prop_info["type"]
        if prop_type not in filter_type_map:
            raise ValueError(f"不支持的频率字段类型: {prop_type} (支持类型: {list(filter_type_map.keys())})")

        config = filter_type_map[prop_type]
        filters = []

        for status in self.config["FREQUENCY_STATUS"]:
            filter_condition = {
                "property": self.config["FREQUENCY_NAME"],
                config["type"]: {config["operator"]: config["value_mapper"](status)}
            }
            # 多选类型特殊处理
            if prop_type == "multi_select":
                filter_condition["multi_select"]["contains"] = status
            filters.append(filter_condition)

        return {"or": filters} if len(filters) > 1 else filters[0]

    def _prepare_update_data(self, page: dict) -> Dict[str, Any]:
        """准备更新数据"""
        properties = {
            col: {"status": {"id": self._get_status_id(col)}}
            for col in self.config["STATUS_COLUMNS"]
        }

        if self.config["TIME_COLUMN"]:
            properties[self.config["TIME_COLUMN"]] = {
                "date": self._current_timestamp()
            }

        return {
            "page_id": page["id"],
            "properties": properties,
            "metadata": {
                "title": self._get_page_title(page),
                "url": page.get("url", "")
            }
        }

    @lru_cache(maxsize=50)
    def _get_status_id(self, column: str) -> str:
        """获取状态ID（带缓存）"""
        schema = self._get_db_schema()
        options = schema["properties"][column]["status"]["options"]
        for opt in options:
            if opt["name"] == self.config["TARGET_STATUS"]:
                return opt["id"]
        raise ValueError(f"状态列 {column} 中未找到 {self.config['TARGET_STATUS']}")

    def batch_process_tasks(self):
        """批量处理任务主流程"""
        try:
            # 分页查询
            query = {
                "database_id": self.config["NOTION_DATABASE_ID"],
                "filter": self._build_frequency_filter(),
                "page_size": 100
            }

            # 处理分页
            success, total = 0, 0
            while True:
                response = self.notion.databases.query( ** query)
                pages = response.get("results", [])
                total += len(pages)

                for page in pages:
                    update_data = self._prepare_update_data(page)
                    try:
                        self.notion.pages.update( ** update_data)
                        success += 1
                        logger.info(f"更新成功: {update_data['metadata']['title']}")
                    except APIResponseError as e:
                        logger.error(f"更新失败 {update_data['metadata']['url']}: {e.body}")

                if not response.get("has_more"):
                    break
                query["start_cursor"] = response["next_cursor"]

            logger.info(f"处理完成 | 总数: {total} | 成功: {success} | 失败: {total - success}")

        except Exception as e:
            logger.error(f"流程中断: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def _current_timestamp() -> dict:
        """生成标准时间戳"""
        now = datetime.datetime.now(datetime.timezone.utc)
        return {
            "start": now.isoformat(),
            "time_zone": "Etc/GMT"
        }

    @staticmethod
    def _get_page_title(page: dict) -> str:
        """提取页面标题"""
        title_prop = page["properties"].get("任务名称", {})
        return title_prop.get("title", [{}])[0].get("plain_text", "无标题")

    @staticmethod
    def _parse_list(value: str) -> List[str]:
        """解析列表配置"""
        return [v.strip() for v in (value or "").split(",") if v.strip()]


if __name__ == "__main__":
    try:
        manager = NotionTaskManager()
        manager.batch_process_tasks()
    except Exception as e:
        logger.error(f"程序终止: {str(e)}")
        exit(1)