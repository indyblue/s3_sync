module.exports = {
  mode: 'production',
  devtool: 'source-map',
  entry: {
    'my-preact': './preactSrc.js',
  },
  output: {
    libraryTarget: 'window',
  },
  module: {
    rules: [
      {
        test: /\.m?js$/,
        exclude: /(node_modules|bower_components)/,
        use: {
          loader: 'babel-loader',
          options: {
            presets: ['@babel/preset-env'],
          },
        },
      },
    ],
  },
};
