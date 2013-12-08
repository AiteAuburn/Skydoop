package org.liang.Visual;

import java.awt.Dimension;

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.List;

import javax.swing.*;

import java.awt.geom.Rectangle2D;
import java.awt.geom.Ellipse2D;
import java.awt.Color;

import org.liang.DataStructures.instance;


public class MinMaxVisual extends JFrame {
	public MinMaxVisual() {
		super("Game Frame");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		pack();
		setLocationRelativeTo(null);
		setVisible(true);
	}

	public MinMaxVisual(List<instance.point> minList, List<instance.point> maxList) {
		super("Min Max Visual");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		Squares sqs = new Squares();
		getContentPane().add(sqs);
		for (int i = 0; i < minList.size(); i++) {
			instance.point currMin = minList.get(i);
			sqs.addMin(currMin.__coordinates[0]  * 600.0, 600.0 - currMin.__coordinates[1] * 600.0 );
		}

		for (int i = 0; i < maxList.size(); i++) {
			instance.point currMax = maxList.get(i);
			sqs.addMax(currMax.__coordinates[0]  * 600.0, 600.0 - currMax.__coordinates[1] * 600.0 );
		}
		pack();
		setLocationRelativeTo(null);
		setVisible(true);
	}


	public static void main(String[] args) {
		new InstVisualization();
	}

}

class Squares extends JPanel {
	private static final int PREF_W = 800;
	private static final int PREF_H = PREF_W;
	private List<Rectangle2D.Double> Rectangles = new ArrayList<Rectangle2D.Double>();
	private List<Ellipse2D.Double> ellipses = new ArrayList<Ellipse2D.Double>();
	private List<Ellipse2D.Double> ellipsesMin = new ArrayList<Ellipse2D.Double>();
	private List<Ellipse2D.Double> ellipsesMax = new ArrayList<Ellipse2D.Double>();


	public void addRectangle(double x, double y, double w, double h) {
		Rectangle2D.Double rect = new Rectangle2D.Double(x, y, w, h);
		Rectangles.add(rect);
	}

	public void addMin(double x, double y) {
		Ellipse2D.Double e1 = new Ellipse2D.Double(x, y, 10.0, 6.0);
		ellipsesMin.add(e1);
	}

	public void addMax(double x, double y) {
		Ellipse2D.Double e1 = new Ellipse2D.Double(x, y, 10.0, 6.0);
		ellipsesMax.add(e1);
	}

	@Override
		public Dimension getPreferredSize() {
			return new Dimension(PREF_W, PREF_H);
		}


	@Override
		protected void paintComponent(Graphics g) {
			super.paintComponent(g);
			Graphics2D g2 = (Graphics2D) g;
			for (Rectangle2D rect : Rectangles) {
				g2.draw(rect);
			}
			
			g2.setPaint(Color.blue);
			for (Ellipse2D el : ellipses) { 
				g2.draw(el);
			}

			g2.setPaint(Color.green);
			for (Ellipse2D el : ellipsesMin) { 
				g2.draw(el);
			}
			g2.setPaint(Color.red);
			for (Ellipse2D el : ellipsesMax) { 
				g2.draw(el);
			}


		}
}
