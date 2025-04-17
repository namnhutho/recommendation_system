from flask import Blueprint, jsonify
import pandas as pd
from recommend.recommendation_model import get_hybrid_recommendations, load_weights_from_minio
from recommend.trending_popular import get_trending_items, get_popular_items, get_latest_metadata, get_diversified_trending_popular

recommend_bp = Blueprint('recommend_bp', __name__)

# Load sẵn để dùng lại
df_metadata = get_latest_metadata()
df_weight = load_weights_from_minio()

@recommend_bp.route('/recommend-weighted/<user_id>', methods=['GET'])
def recommend_weighted(user_id):
    try:
        top_k = 10
        user_id = int(user_id)
        user_history = df_metadata[df_metadata['user_id'] == user_id]

        if user_history.empty:
            print(f"[INFO] User {user_id} là user mới - không có tương tác.")
            trending_df = get_trending_items(df_metadata, top_k)
            popular_df = get_popular_items(df_metadata, top_k)

            num_each = top_k // 2
            trending_sample = trending_df.sample(n=num_each, random_state=42)
            popular_sample = popular_df.sample(n=top_k - num_each, random_state=42)
            combined_df = pd.concat([trending_sample, popular_sample]).drop_duplicates(subset='product_id')

        elif len(user_history) < 6:
            print(f"[INFO] User {user_id} là user cold-start - ít hơn 6 tương tác.")
            trending_df = get_trending_items(df_metadata, top_k)
            popular_df = get_popular_items(df_metadata, top_k)
            cbf_recommendations = get_hybrid_recommendations(user_id, df_weight, k=top_k)
            if hasattr(cbf_recommendations, "toPandas"):
                cbf_recommendations = cbf_recommendations.toPandas()

            n_cbf = int(top_k * 0.4)
            n_trending = int(top_k * 0.3)
            n_popular = top_k - n_cbf - n_trending

            cbf_sample = cbf_recommendations.sample(n=n_cbf, random_state=42)
            trending_sample = trending_df.sample(n=n_trending, random_state=42)
            popular_sample = popular_df.sample(n=n_popular, random_state=42)

            combined_df = pd.concat([cbf_sample, trending_sample, popular_sample]).drop_duplicates(subset='product_id')

        else:
            print(f"[INFO] User {user_id} đã có đủ dữ liệu - dùng mô hình hybrid.")
            hybrid_recommendations = get_hybrid_recommendations(user_id, df_weight, k=top_k)
            if hasattr(hybrid_recommendations, "toPandas"):
                hybrid_recommendations = hybrid_recommendations.toPandas()
            combined_df = hybrid_recommendations

        metadata_df = df_metadata[['product_id', 'category_code', 'brand', 'price']].drop_duplicates()
        merged_df = combined_df.merge(metadata_df, on='product_id', how='left')
        merged_df['brand'] = merged_df['brand'].fillna('Unknown')
        merged_df['category_code'] = merged_df['category_code'].fillna('Unknown')

        recommendations_list = []
        for _, row in merged_df.iterrows():
            item = {
                'product_id': row['product_id'],
                'brand': row['brand'],
                'category_code': row['category_code'],
                'price': row['price']
            }

            if 'final_score' in row and pd.notna(row['final_score']):
                item['final_score'] = row['final_score']
            else:
                item['score'] = row.get('score', None)
                item['source'] = row.get('source', None)

            recommendations_list.append(item)

        return jsonify({
            'user_id': user_id,
            'recommendations': recommendations_list
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@recommend_bp.route('/recommend-anonymous', methods=['GET'])
def recommend_for_anonymous():
    try:
        df_metadata = get_latest_metadata()
        recommended_df = get_diversified_trending_popular(df_metadata, top_k_each=5)

        # Lấy metadata cần thiết
        metadata_df = df_metadata[['product_id', 'category_code', 'brand', 'price']].drop_duplicates()
        merged_df = recommended_df.merge(metadata_df, on='product_id', how='left')

        # Bổ sung giá trị mặc định nếu thiếu
        merged_df['brand'] = merged_df['brand'].fillna('Unknown')
        merged_df['category_code'] = merged_df['category_code'].fillna('Unknown')

        recommendations_list = []
        for _, row in merged_df.iterrows():
            item = {
                'product_id': row['product_id'],
                'brand': row['brand'],
                'category_code': row['category_code'],
                'price': row['price'],
                'score': row['score'],
                'source': row['source']
            }
            recommendations_list.append(item)

        return jsonify({
            'user_id': 'anonymous',
            'recommendations': recommendations_list
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500
