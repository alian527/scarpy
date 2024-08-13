import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 读取合并后的数据
combined_df = pd.read_excel("合并.xlsx")

# 创建一个包含三个子图的图形
fig, axs = plt.subplots(1, 3, figsize=(18, 6))

# 子图1：性别比例图
gender_counts = combined_df['gender'].value_counts()
axs[0].pie(gender_counts, autopct='%1.1f%%', colors=['skyblue', 'lightcoral'], labels=['Male', 'Female'], startangle=140)
axs[0].set_title('Gender Distribution')

# 子图2：男女发帖数差别小提琴图
sns.violinplot(x='gender', y='num_posts', data=combined_df, palette=['skyblue', 'lightcoral'], ax=axs[1])
axs[1].set_title('Distribution of Number of Posts by Gender')
axs[1].set_xlabel('Gender')
axs[1].set_ylabel('Number of Posts')

# 子图3：男女吧龄差别图
mean_years = combined_df.groupby('gender')['years_on_tieba'].mean()
bars = axs[2].bar(['Male', 'Female'], mean_years, color=['skyblue', 'lightcoral'])

# 标注人数
for bar, count in zip(bars, gender_counts):
    axs[2].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1, f'N={count}', ha='center', va='bottom')

axs[2].set_title('Average Years on Tieba by Gender')
axs[2].set_xlabel('Gender')
axs[2].set_ylabel('Average Years on Tieba')

# 调整子图之间的间距
plt.tight_layout()
plt.show()
