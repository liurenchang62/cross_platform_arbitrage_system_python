# category_mapper.py
#! 类别映射模块：负责将事件标题映射到预定义的类别
#
# 从 categories.toml 加载配置，提供多类别判断功能

import toml
import os
from datetime import datetime
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class CategoryConfig:
    """类别配置"""
    name: str
    keywords: List[str]
    weight: float
    description: Optional[str] = None


@dataclass
class CategoryMapperConfig:
    """类别映射器配置集合"""
    categories: List[CategoryConfig] = field(default_factory=list)


class CategoryMapper:
    """类别映射器"""

    def __init__(self, config_path: Optional[str] = None):
        self.config = CategoryMapperConfig()
        self.keyword_to_categories: Dict[str, List[str]] = {}
        self.category_names: Set[str] = set()
        self.config_path = config_path or ""
        self.last_modified: Optional[datetime] = None

        if config_path and os.path.exists(config_path):
            self._load_from_file(config_path)

    @classmethod
    def from_file(cls, path: str) -> 'CategoryMapper':
        """从文件创建新的类别映射器"""
        mapper = cls()
        mapper._load_from_file(path)
        return mapper

    @classmethod
    def default(cls) -> 'CategoryMapper':
        """创建默认的空映射器"""
        return cls()

    def _load_from_file(self, path: str):
        """从文件加载配置"""
        self.config_path = path

        # 读取文件
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 获取最后修改时间
        self.last_modified = datetime.fromtimestamp(os.path.getmtime(path))

        # 解析 TOML
        data = toml.loads(content)

        # 构建配置
        categories = []
        for cat_data in data.get("categories", []):
            category = CategoryConfig(
                name=cat_data["name"],
                keywords=cat_data["keywords"],
                weight=cat_data.get("weight", 1.0),
                description=cat_data.get("description")
            )
            categories.append(category)

        self.config = CategoryMapperConfig(categories=categories)
        self._build_index()

    def _build_index(self):
        """构建关键词反向索引"""
        self.keyword_to_categories.clear()
        self.category_names.clear()

        for category in self.config.categories:
            self.category_names.add(category.name)

            for keyword in category.keywords:
                keyword_lower = keyword.lower()
                if keyword_lower not in self.keyword_to_categories:
                    self.keyword_to_categories[keyword_lower] = []
                self.keyword_to_categories[keyword_lower].append(category.name)

    def check_reload(self) -> bool:
        """检查配置文件是否已更新（热加载）"""
        if not self.config_path or not os.path.exists(self.config_path):
            return False

        current_modified = datetime.fromtimestamp(os.path.getmtime(self.config_path))

        if current_modified != self.last_modified:
            # 文件已修改，重新加载
            self._load_from_file(self.config_path)
            print(f"🔄 类别配置已热加载: {self.config_path}")
            return True

        return False

    def classify(self, text: str) -> List[str]:
        """判断文本属于哪些类别"""
        text_lower = text.lower()
        matched_categories = set()

        # 遍历所有关键词，检查是否出现在文本中
        for keyword, categories in self.keyword_to_categories.items():
            if keyword in text_lower:
                for category in categories:
                    matched_categories.add(category)

        result = sorted(list(matched_categories))
        return result

    def get_all_categories(self) -> Set[str]:
        """获取所有类别名称"""
        return self.category_names.copy()

    def get_category_config(self, name: str) -> Optional[CategoryConfig]:
        """获取类别配置"""
        for cat in self.config.categories:
            if cat.name == name:
                return cat
        return None

    def has_any_category(self, text: str) -> bool:
        """检查文本是否有任何类别匹配"""
        return len(self.classify(text)) > 0

    def extract_keywords_for_log(self, text: str) -> List[str]:
        """获取未分类的文本（返回提取的关键词）"""
        text_lower = text.lower()
        keywords = []

        # 提取所有可能的关键词（长度>3的词）
        for word in text_lower.split():
            # 清理标点符号
            clean_word = word.strip('.,!?;:()[]{}"\'')
            if len(clean_word) > 3 and clean_word not in self.keyword_to_categories:
                keywords.append(clean_word)

        keywords = sorted(list(set(keywords)))
        return keywords[:10]  # 最多保留10个关键词


# 全局单例类别映射器（可选，用于需要全局访问的场景）
_GLOBAL_CATEGORY_MAPPER: Optional[CategoryMapper] = None


def get_global_mapper() -> Optional[CategoryMapper]:
    """获取全局类别映射器"""
    return _GLOBAL_CATEGORY_MAPPER


def init_global_mapper(path: str) -> CategoryMapper:
    """初始化全局类别映射器"""
    global _GLOBAL_CATEGORY_MAPPER
    mapper = CategoryMapper.from_file(path)
    _GLOBAL_CATEGORY_MAPPER = mapper
    return mapper